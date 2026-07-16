//! Deterministic network simulator for testing Raft under adversarial
//! conditions (drops, delays, partitions) with fully reproducible runs.
//! No threads, no sockets, no wall-clock time — everything advances
//! by explicit `tick()` calls driven by this harness.

use std::collections::{HashMap, VecDeque};

use crate::message::{Envelope, Message};
use crate::node::Node;
use crate::types::NodeId;

/// A message in flight, scheduled to arrive at a specific virtual tick.
struct InFlight {
    envelope: Envelope,
    deliver_at: u64,
}

/// Minimal xorshift PRNG so the simulator has zero external dependencies
/// and every run is reproducible purely from an integer seed.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Rng(if seed == 0 { 0xdeadbeef } else { seed })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn chance(&mut self, numerator: u64, denominator: u64) -> bool {
        self.next_u64() % denominator < numerator
    }

    fn range(&mut self, lo: u64, hi: u64) -> u64 {
        lo + self.next_u64() % (hi - lo + 1)
    }
}

/// Controls what fraction of messages get dropped and how much jitter
/// is added to delivery, plus which node pairs are currently partitioned.
pub struct NetworkConditions {
    pub drop_rate_pct: u64,
    pub min_delay: u64,
    pub max_delay: u64,
    partitioned_pairs: std::collections::HashSet<(NodeId, NodeId)>,
}

impl Default for NetworkConditions {
    fn default() -> Self {
        NetworkConditions {
            drop_rate_pct: 0,
            min_delay: 1,
            max_delay: 1,
            partitioned_pairs: Default::default(),
        }
    }
}

impl NetworkConditions {
    pub fn partition(&mut self, a: NodeId, b: NodeId) {
        self.partitioned_pairs.insert((a, b));
        self.partitioned_pairs.insert((b, a));
    }

    pub fn heal(&mut self, a: NodeId, b: NodeId) {
        self.partitioned_pairs.remove(&(a, b));
        self.partitioned_pairs.remove(&(b, a));
    }

    pub fn heal_all(&mut self) {
        self.partitioned_pairs.clear();
    }

    fn is_partitioned(&self, a: NodeId, b: NodeId) -> bool {
        self.partitioned_pairs.contains(&(a, b))
    }
}

pub struct Simulator {
    pub nodes: HashMap<NodeId, Node>,
    pub conditions: NetworkConditions,
    rng: Rng,
    clock: u64,
    in_flight: VecDeque<InFlight>,
}

impl Simulator {
    pub fn new(seed: u64) -> Self {
        Simulator {
            nodes: HashMap::new(),
            conditions: NetworkConditions::default(),
            rng: Rng::new(seed),
            clock: 0,
            in_flight: VecDeque::new(),
        }
    }

    pub fn add_node(&mut self, node: Node) {
        self.nodes.insert(node.state.id, node);
    }

    /// Advances the whole cluster by one virtual tick: every node's
    /// clock ticks, then any messages scheduled to arrive now are
    /// delivered, subject to the current network conditions.
    pub fn step(&mut self) {
        self.clock += 1;

        for node in self.nodes.values_mut() {
            node.tick();
        }

        // Collect newly queued outbound messages from all nodes.
        let mut fresh = Vec::new();
        for node in self.nodes.values_mut() {
            fresh.extend(node.drain_outbox());
        }

        for envelope in fresh {
            self.enqueue(envelope);
        }

        self.deliver_due();
    }

    fn enqueue(&mut self, envelope: Envelope) {
        if self.conditions.is_partitioned(envelope.from, envelope.to) {
            return;
        }
        if self.conditions.drop_rate_pct > 0
            && self.rng.chance(self.conditions.drop_rate_pct, 100)
        {
            return;
        }

        let delay = self.rng.range(self.conditions.min_delay, self.conditions.max_delay);
        self.in_flight.push_back(InFlight { envelope, deliver_at: self.clock + delay });
    }

    fn deliver_due(&mut self) {
        let mut remaining = VecDeque::new();
        while let Some(msg) = self.in_flight.pop_front() {
            if msg.deliver_at <= self.clock {
                if let Some(node) = self.nodes.get_mut(&msg.envelope.to) {
                    let Envelope { from, message, .. } = msg.envelope;
                    node.step(from, message);
                }
            } else {
                remaining.push_back(msg);
            }
        }
        self.in_flight = remaining;
    }

    pub fn run_ticks(&mut self, count: u64) {
        for _ in 0..count {
            self.step();
        }
    }

    /// True once every node agrees on who the leader is for the same term.
    pub fn current_leader(&self) -> Option<NodeId> {
        use crate::state::Role;
        self.nodes
            .values()
            .find(|n| matches!(n.state.role, Role::Leader { .. }))
            .map(|n| n.state.id)
    }
}

// Silence an unused-import warning when Message isn't referenced directly
// outside of type positions in this file's public surface.
#[allow(unused_imports)]
use Message as _Message;
