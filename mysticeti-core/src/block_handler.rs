// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::commit_interpreter::{CommitInterpreter, CommittedSubDag};
use crate::committee::{Committee, QuorumThreshold, TransactionAggregator};
use crate::config::StorageDir;
use crate::data::Data;
use crate::log::CertifiedTransactionLog;
use crate::runtime::TimeInstant;
use crate::stat::PreciseHistogram;
use crate::syncer::CommitObserver;
use crate::types::{
    AuthorityIndex, BaseStatement, BlockReference, StatementBlock, Transaction, TransactionId,
};
use crate::{
    block_store::{BlockStore, CommitData},
    metrics::Metrics,
};
use minibytes::Bytes;
use parking_lot::Mutex;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

pub trait BlockHandler: Send + Sync {
    fn handle_blocks(&mut self, blocks: &[Data<StatementBlock>]) -> Vec<BaseStatement>;

    fn state(&self) -> Bytes;

    fn recover_state(&mut self, _state: &Bytes);
}

pub struct RealBlockHandler {
    transaction_votes:
        TransactionAggregator<TransactionId, QuorumThreshold, CertifiedTransactionLog>,
    pub transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
    committee: Arc<Committee>,
    authority: AuthorityIndex,
    pub transaction_certified_latency: PreciseHistogram<Duration>,
    rng: StdRng,
}

impl RealBlockHandler {
    pub fn new(committee: Arc<Committee>, authority: AuthorityIndex, config: &StorageDir) -> Self {
        let rng = StdRng::seed_from_u64(authority);
        let transaction_log = CertifiedTransactionLog::start(config.certified_transactions_log())
            .expect("Failed to open certified transaction log for write");
        Self {
            transaction_votes: TransactionAggregator::with_handler(transaction_log),
            transaction_time: Default::default(),
            committee,
            authority,
            transaction_certified_latency: Default::default(),
            rng,
        }
    }
}

impl BlockHandler for RealBlockHandler {
    fn handle_blocks(&mut self, blocks: &[Data<StatementBlock>]) -> Vec<BaseStatement> {
        let mut response = vec![];
        let next_transaction = self.rng.next_u64();
        response.push(BaseStatement::Share(
            next_transaction,
            Transaction::new(next_transaction.to_le_bytes().to_vec()),
        ));
        let mut transaction_time = self.transaction_time.lock();
        transaction_time.insert(next_transaction, TimeInstant::now());
        self.transaction_votes
            .register(next_transaction, self.authority, &self.committee);
        for block in blocks {
            let processed =
                self.transaction_votes
                    .process_block(block, Some(&mut response), &self.committee);
            for processed_id in processed {
                if let Some(instant) = transaction_time.get(&processed_id) {
                    self.transaction_certified_latency
                        .observe(instant.elapsed());
                }
            }
        }
        response
    }

    fn state(&self) -> Bytes {
        self.transaction_votes.state()
    }

    fn recover_state(&mut self, state: &Bytes) {
        self.transaction_votes.with_state(state);
    }
}

// Immediately votes and generates new transactions
pub struct TestBlockHandler {
    pub last_transaction: u64,
    transaction_votes: TransactionAggregator<TransactionId, QuorumThreshold>,
    pub transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
    committee: Arc<Committee>,
    authority: AuthorityIndex,

    pub transaction_certified_latency: PreciseHistogram<Duration>,
}

impl TestBlockHandler {
    pub fn new(
        last_transaction: u64,
        committee: Arc<Committee>,
        authority: AuthorityIndex,
    ) -> Self {
        Self {
            last_transaction,
            transaction_votes: Default::default(),
            transaction_time: Default::default(),
            committee,
            authority,
            transaction_certified_latency: Default::default(),
        }
    }

    pub fn is_certified(&self, txid: TransactionId) -> bool {
        self.transaction_votes.is_processed(&txid)
    }
}

impl BlockHandler for TestBlockHandler {
    fn handle_blocks(&mut self, blocks: &[Data<StatementBlock>]) -> Vec<BaseStatement> {
        // todo - this is ugly, but right now we need a way to recover self.last_transaction
        for block in blocks {
            if block.author() == self.authority {
                // We can see our own block in handle_blocks - this can happen during core recovery
                // Todo - we might also need to process pending Payload statements as well
                for statement in block.statements() {
                    if let BaseStatement::Share(id, _) = statement {
                        self.last_transaction = max(self.last_transaction, *id);
                    }
                }
            }
        }
        let mut response = vec![];
        self.last_transaction += 1;
        response.push(BaseStatement::Share(
            self.last_transaction,
            Transaction::new(self.last_transaction.to_le_bytes().to_vec()),
        ));
        let mut transaction_time = self.transaction_time.lock();
        transaction_time.insert(self.last_transaction, TimeInstant::now());
        self.transaction_votes
            .register(self.last_transaction, self.authority, &self.committee);
        for block in blocks {
            println!("Processing {block:?}");
            let processed =
                self.transaction_votes
                    .process_block(block, Some(&mut response), &self.committee);
            for processed_id in processed {
                if let Some(instant) = transaction_time.get(&processed_id) {
                    self.transaction_certified_latency
                        .observe(instant.elapsed());
                }
            }
        }
        response
    }

    fn state(&self) -> Bytes {
        let state = (&self.transaction_votes.state(), &self.last_transaction);
        let bytes =
            bincode::serialize(&state).expect("Failed to serialize transaction aggregator state");
        bytes.into()
    }

    fn recover_state(&mut self, state: &Bytes) {
        let (transaction_votes, last_transaction) = bincode::deserialize(state)
            .expect("Failed to deserialize transaction aggregator state");
        self.transaction_votes.with_state(&transaction_votes);
        self.last_transaction = last_transaction;
    }
}

#[allow(dead_code)]
pub struct TestCommitHandler {
    commit_interpreter: CommitInterpreter,
    transaction_votes: TransactionAggregator<TransactionId, QuorumThreshold>,
    committee: Arc<Committee>,
    committed_leaders: Vec<BlockReference>,
    committed_dags: Vec<CommittedSubDag>,

    start_time: TimeInstant,
    transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
    pub certificate_committed_latency: PreciseHistogram<Duration>,
    pub transaction_committed_latency: PreciseHistogram<Duration>,

    metrics: Arc<Metrics>,
}

impl TestCommitHandler {
    pub fn new(
        committee: Arc<Committee>,
        transaction_time: Arc<Mutex<HashMap<TransactionId, TimeInstant>>>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            commit_interpreter: CommitInterpreter::new(),
            transaction_votes: Default::default(),
            committee,
            committed_leaders: vec![],
            committed_dags: vec![],

            start_time: TimeInstant::now(),
            transaction_time,
            certificate_committed_latency: Default::default(),
            transaction_committed_latency: Default::default(),

            metrics,
        }
    }

    pub fn committed_leaders(&self) -> &Vec<BlockReference> {
        &self.committed_leaders
    }

    /// Note: these metrics are used to compute performance during benchmarks.
    fn update_metrics(&self, timestamp: &Duration) {
        let time_from_start = self.start_time.elapsed();
        let benchmark_duration = self.metrics.benchmark_duration.get();
        if let Some(delta) = time_from_start.as_secs().checked_sub(benchmark_duration) {
            self.metrics.benchmark_duration.inc_by(delta);
        }

        self.metrics
            .latency_s
            .with_label_values(&["default"])
            .observe(timestamp.as_secs_f64());

        let square_latency = timestamp.as_secs_f64().powf(2.0);
        self.metrics
            .latency_squared_s
            .with_label_values(&["default"])
            .inc_by(square_latency);
    }
}

impl CommitObserver for TestCommitHandler {
    fn handle_commit(
        &mut self,
        block_store: &BlockStore,
        committed_leaders: Vec<Data<StatementBlock>>,
    ) -> Vec<CommitData> {
        let committed = self
            .commit_interpreter
            .handle_commit(block_store, committed_leaders);
        let transaction_time = self.transaction_time.lock();
        let mut commit_data = vec![];
        for commit in committed {
            self.committed_leaders.push(commit.anchor);
            for block in &commit.blocks {
                let processed = self
                    .transaction_votes
                    .process_block(block, None, &self.committee);
                for processed_id in processed {
                    if let Some(instant) = transaction_time.get(&processed_id) {
                        self.certificate_committed_latency
                            .observe(instant.elapsed());
                    }
                }
                for statement in block.statements() {
                    if let BaseStatement::Share(id, _) = statement {
                        if let Some(instant) = transaction_time.get(id) {
                            let timestamp = instant.elapsed();
                            self.update_metrics(&timestamp);
                            self.transaction_committed_latency.observe(timestamp);
                        }
                    }
                }
            }
            commit_data.push(CommitData::from(&commit));
            self.committed_dags.push(commit);
        }
        commit_data
    }

    fn aggregator_state(&self) -> Bytes {
        self.transaction_votes.state()
    }

    fn recover_committed(&mut self, committed: HashSet<BlockReference>, state: Option<Bytes>) {
        assert!(self.commit_interpreter.committed.is_empty());
        if let Some(state) = state {
            self.transaction_votes.with_state(&state);
        } else {
            assert!(committed.is_empty());
        }
        self.commit_interpreter.committed = committed;
    }
}
