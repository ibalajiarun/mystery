// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::ops::{AddAssign, Div};
use tokio::sync::mpsc;

pub struct PreciseHistogram<T> {
    points: Vec<T>, // todo - we need to reset this vector periodically
    sum: T,
    receiver: mpsc::UnboundedReceiver<T>,
}

#[derive(Clone)]
pub struct HistogramSender<T> {
    sender: mpsc::UnboundedSender<T>,
}

pub fn histogram<T: Default>() -> (PreciseHistogram<T>, HistogramSender<T>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let sender = HistogramSender { sender };
    let histogram = PreciseHistogram {
        points: Default::default(),
        sum: Default::default(),
        receiver,
    };
    (histogram, sender)
}

impl<T: Send> HistogramSender<T> {
    pub fn observe(&self, t: T) {
        self.sender.send(t).ok();
    }
}

impl<T: Ord + AddAssign + Div<u32, Output = T> + Copy + Default> PreciseHistogram<T> {
    pub fn observe(&mut self, point: T) {
        self.points.push(point);
        self.sum += point;
    }

    pub fn avg(&self) -> Option<T> {
        if self.points.is_empty() {
            return None;
        }
        Some(self.sum / self.points.len() as u32)
    }

    pub fn pcts<const N: usize>(&mut self, pct: [usize; N]) -> Option<[T; N]> {
        self.receive_all();
        if self.points.is_empty() {
            return None;
        }
        // Current sort algorithm in rust works faster on pre-sorted data.
        // So we sort inside current vector, instead of cloning a new one every time,
        // to make subsequent calls faster.
        self.points.sort();
        let mut result = [T::default(); N];
        for (i, pct) in pct.iter().enumerate() {
            result[i] = *self.points.get(self.pct1000_index(*pct)).unwrap();
        }
        Some(result)
    }

    pub fn pct(&mut self, pct1000: usize) -> Option<T> {
        self.pcts([pct1000]).map(|[p]| p)
    }

    fn receive_all(&mut self) {
        while let Ok(d) = self.receiver.try_recv() {
            self.points.push(d);
        }
    }

    fn pct1000_index(&self, pct1000: usize) -> usize {
        debug_assert!(pct1000 < 1000);
        self.points.len() * pct1000 / 1000
    }
}
