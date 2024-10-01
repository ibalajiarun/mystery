// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use core::fmt;
use std::str;

use futures::future::try_join_all;
use serde::Serialize;
use tokio::process::Command;

use super::{Instance, ServerProviderClient};
use crate::{
    error::{CloudProviderError, CloudProviderResult},
    settings::Settings,
};

pub struct GcpClient {
    /// The settings of the testbed.
    settings: Settings,
}

impl fmt::Display for GcpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GCP")
    }
}

impl GcpClient {
    pub fn new(settings: Settings) -> Self {
        Self { settings }
    }

    /// Convert a GCP instance into an orchestrator instance (used in the rest of the codebase).
    fn make_instance(&self, region: String, gcp_instance: &serde_json::Value) -> Instance {
        Instance {
            id: gcp_instance["name"]
                .as_str()
                .expect("GCP instance should have an name")
                .into(),
            region,
            main_ip: gcp_instance["networkInterfaces"][0]["accessConfigs"][0]["natIP"]
                .as_str()
                .unwrap_or("0.0.0.0") // Stopped instances do not have an ip address.
                .parse()
                .expect("GCP instance should have a valid ip"),
            tags: vec![self.settings.testbed_id.clone()],
            specs: format!(
                "{}",
                gcp_instance["machineType"]
                    .as_str()
                    .expect("GCP instance should have a type")
                    .split('/')
                    .last()
                    .unwrap()
            ),
            status: format!(
                "{}",
                gcp_instance["status"]
                    .as_str()
                    .expect("GCP instance should have a status")
            )
            .as_str()
            .into(),
        }
    }

    /// Create a new firewall rule for the instance (if it doesn't already exist).
    async fn create_firewall_rule(&self) -> CloudProviderResult<()> {
        // Create a firewall rule (if it doesn't already exist).
        let firewall_name = format!("{}-firewall", &self.settings.testbed_id);
        let output = Command::new("gcloud")
            .args(&[
                "compute",
                "firewall-rules",
                "create",
                &firewall_name,
                "--allow",
                "tcp,udp,icmp",
                "--source-ranges",
                "0.0.0.0/0",
                "--description",
                "Allow all traffic (used for benchmarks).",
                "--target-tags",
                &firewall_name,
            ])
            .output()
            .await
            .expect("Failed to execute command");

        if !output.status.success() {
            let error_message = str::from_utf8(&output.stderr).unwrap();
            if !error_message.to_lowercase().contains("already exists") {
                return Err(CloudProviderError::UnexpectedResponse(format!(
                    "Failed to create firewall rule: {}",
                    error_message
                )));
            }
        }

        Ok(())
    }

    /// Return the command to mount the first (standard) NVMe drive.
    fn nvme_mount_command(&self) -> Vec<String> {
        const DRIVE: &str = "nvme0n1";
        let directory = self.settings.working_dir.display();
        vec![
            format!("(sudo mkfs.ext4 -E nodiscard /dev/{DRIVE} || true)"),
            format!("(sudo mount /dev/{DRIVE} {directory} || true)"),
            format!("sudo chmod 777 -R {directory}"),
        ]
    }

    fn nvme_unmount_command(&self) -> Vec<String> {
        let directory = self.settings.working_dir.display();
        vec![format!("(sudo umount {directory} || true)")]
    }
}

impl ServerProviderClient for GcpClient {
    const USERNAME: &'static str = "balaji";

    async fn list_instances(&self) -> CloudProviderResult<Vec<Instance>> {
        let mut instances = Vec::new();
        for region in &self.settings.regions {
            let output = Command::new("gcloud")
                .args(&[
                    "compute",
                    "instances",
                    "list",
                    "--format",
                    "json",
                    "--filter",
                    &format!(
                        "tags.items={} AND zone:({})",
                        self.settings.testbed_id, region
                    ),
                ])
                .output()
                .await
                .expect("Failed to execute command");
            let gcp_instances: Vec<serde_json::Value> = serde_json::from_slice(&output.stdout)?;
            for instance in gcp_instances {
                instances.push(self.make_instance(region.clone(), &instance));
            }
        }

        Ok(instances)
    }

    async fn start_instances<'a, I>(&self, instances: I) -> CloudProviderResult<()>
    where
        I: Iterator<Item = &'a Instance> + Send,
    {
        let mut fut_vec = Vec::new();
        for instance in instances {
            let fut = Command::new("gcloud")
                .args(&[
                    "compute",
                    "instances",
                    "start",
                    &instance.id,
                    "--zone",
                    &instance.region,
                ])
                .output();
            fut_vec.push(fut);
        }

        try_join_all(fut_vec)
            .await
            .expect("Failed to execute command");

        Ok(())
    }

    async fn stop_instances<'a, I>(&self, instances: I) -> CloudProviderResult<()>
    where
        I: Iterator<Item = &'a Instance> + Send,
    {
        let mut fut_vec = Vec::new();
        for instance in instances {
            let fut = Command::new("gcloud")
                .args(&[
                    "compute",
                    "instances",
                    "stop",
                    &instance.id,
                    "--zone",
                    &instance.region,
                    "--discard-local-ssd=true",
                ])
                .output();
            fut_vec.push(fut);
        }

        try_join_all(fut_vec)
            .await
            .expect("Failed to execute command");

        Ok(())
    }

    async fn create_instance<S>(&self, region: S) -> CloudProviderResult<Instance>
    where
        S: Into<String> + Serialize + Send,
    {
        let region = region.into();
        // Generate a unique 4-character identifier for the testbed and the region.
        let random_id = rand::random::<u16>();
        let testbed_id = &self.settings.testbed_id;
        let instance_id = format!("{}-{}", testbed_id, random_id);

        // Create a firewall rule (if needed).
        self.create_firewall_rule().await?;

        // Create a new instance.
        const OS_IMAGE: &str = "ubuntu-2004-lts";

        let output = Command::new("gcloud")
            .args(&[
                "compute",
                "instances",
                "create",
                &instance_id,
                "--image-family",
                OS_IMAGE,
                "--image-project",
                "ubuntu-os-cloud",
                "--boot-disk-size",
                "200GB", // Default boot disk size
                "--local-ssd",
                "interface=nvme,size=375GB", // Use local SSD
                "--local-ssd",
                "interface=nvme,size=375GB", // Use local SSD
                "--machine-type",
                &self.settings.specs,
                "--zone",
                &region,
                "--tags",
                &format!("{},{}-firewall", testbed_id, testbed_id),
                "--format",
                "json",
            ])
            .output()
            .await
            .expect("Failed to execute command");
        if !output.status.success() {
            return Err(CloudProviderError::FailureResponseCode(
                format!("{:?}", output.status.code()),
                format!("{}", String::from_utf8(output.stderr).unwrap()),
            ));
        }
        let gcp_instance: serde_json::Value = serde_json::from_slice(&output.stdout)?;
        let gcp_instance = &gcp_instance[0];
        Ok(self.make_instance(region, gcp_instance))
    }

    async fn delete_instance(&self, instance: Instance) -> CloudProviderResult<()> {
        let result = Command::new("gcloud")
            .args(&[
                "compute",
                "instances",
                "delete",
                &instance.id,
                "--zone",
                &instance.region,
                "--delete-disks",
                "all",
                "--quiet",
            ])
            .output()
            .await
            .expect("Failed to execute command");
        assert!(result.status.success(), "Failed to delete instance");

        Ok(())
    }

    async fn register_ssh_public_key(&self, public_key: String) -> CloudProviderResult<()> {
        Command::new("gcloud")
            .args(&[
                "compute",
                "project-info",
                "add-metadata",
                "--metadata",
                &format!("ssh-keys=balaji:{}", public_key),
            ])
            .output()
            .await
            .expect("Failed to execute command");

        Ok(())
    }

    async fn instance_setup_commands(&self) -> CloudProviderResult<Vec<String>> {
        if self.settings.nvme {
            Ok(self.nvme_mount_command())
        } else {
            Ok(self.nvme_unmount_command())
        }
    }
}
