use std::collections::HashMap;

use crate::log::Log;
use crate::message::{
    AppendEntries, AppendEntriesReply, Envelope, Message, RequestVote, RequestVoteReply,
};
use crate::state::{NodeState, Role};
use crate::types::{LogIndex, NodeId, Term};

const DEFAULT_ELECTION_TIMEOUT: u32 = 10;
const HEARTBEAT_INTERVAL: u32 = 2;

/// A single Raft node. Owns no I/O — the driver feeds it ticks and
/// inbound messages, and drains outbound messages + committed entries
/// after each call. This is what makes deterministic simulation possible:
/// the driver fully controls time and message delivery.
pub struct Node {
    pub state: NodeState,
    /// Base election timeout. Actual timeout used each round is this
    /// plus a jittered amount in [0, base), to avoid nodes with similar
    /// timeouts perpetually splitting votes.
    election_timeout_base: u32,
    election_timeout: u32,
    /// Local PRNG used only for timeout jitter, seeded from the node id.
    /// Deliberately separate from any network-level randomness so a
    /// node's jitter behavior doesn't depend on simulator seed.
    rng_state: u64,
    log: Log,
    outbox: Vec<Envelope>,
    pub committed: Vec<(LogIndex, Vec<u8>)>,
}

impl Node {
    pub fn new(id: NodeId, peers: Vec<NodeId>, election_timeout_base: u32) -> Self {
        let seed = id.wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xDEAD_BEEF;
        let mut node = Node {
            state: NodeState::new(id, peers),
            election_timeout_base,
            election_timeout: election_timeout_base,
            rng_state: if seed == 0 { 0xDEAD_BEEF } else { seed },
            log: Log::new(),
            outbox: Vec::new(),
            committed: Vec::new(),
        };
        node.jitter_timeout();
        node
    }

    pub fn with_default_timeout(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self::new(id, peers, DEFAULT_ELECTION_TIMEOUT)
    }

    fn next_rand(&mut self) -> u32 {
        let mut x = self.rng_state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.rng_state = x;
        (x % u64::from(u32::MAX)) as u32
    }

    /// Picks a fresh randomized timeout in [base, 2*base). Called whenever
    /// the election clock is reset, so retried elections don't collide.
    fn jitter_timeout(&mut self) {
        let spread = self.election_timeout_base.max(1);
        let extra = self.next_rand() % spread;
        self.election_timeout = self.election_timeout_base + extra;
    }

    pub fn drain_outbox(&mut self) -> Vec<Envelope> {
        std::mem::take(&mut self.outbox)
    }

    fn send(&mut self, to: NodeId, message: Message) {
        self.outbox.push(Envelope { from: self.state.id, to, message });
    }

    pub fn tick(&mut self) {
        match &self.state.role {
            Role::Leader { .. } => {
                self.state.heartbeat_elapsed += 1;
                if self.state.heartbeat_elapsed >= HEARTBEAT_INTERVAL {
                    self.state.heartbeat_elapsed = 0;
                    self.broadcast_append_entries();
                }
            }
            _ => {
                self.state.election_elapsed += 1;
                if self.state.election_elapsed >= self.election_timeout {
                    self.start_election();
                }
            }
        }
    }

    pub fn step(&mut self, from: NodeId, message: Message) {
        let msg_term = match &message {
            Message::RequestVote(m) => m.term,
            Message::RequestVoteReply(m) => m.term,
            Message::AppendEntries(m) => m.term,
            Message::AppendEntriesReply(m) => m.term,
        };

        if msg_term > self.state.current_term {
            self.become_follower(msg_term);
        }

        match message {
            Message::RequestVote(m) => self.handle_request_vote(from, m),
            Message::RequestVoteReply(m) => self.handle_request_vote_reply(from, m),
            Message::AppendEntries(m) => self.handle_append_entries(from, m),
            Message::AppendEntriesReply(m) => self.handle_append_entries_reply(from, m),
        }
    }

    fn become_follower(&mut self, term: Term) {
        self.state.current_term = term;
        self.state.voted_for = None;
        self.state.role = Role::Follower;
        self.state.election_elapsed = 0;
        self.jitter_timeout();
    }

    fn start_election(&mut self) {
        self.state.current_term += 1;
        self.state.voted_for = Some(self.state.id);
        self.state.election_elapsed = 0;
        self.state.role = Role::Candidate { votes_received: vec![self.state.id] };
        // Re-roll the timeout now, so if this election doesn't reach
        // quorum, the retry wait is a fresh random duration.
        self.jitter_timeout();

        if self.state.cluster_size() == 1 {
            self.become_leader();
            return;
        }

        let request = RequestVote {
            term: self.state.current_term,
            candidate_id: self.state.id,
            last_log_index: self.log.last_index(),
            last_log_term: self.log.last_term(),
        };

        let peers = self.state.peers.clone();
        for peer in peers {
            self.send(peer, Message::RequestVote(request.clone()));
        }
    }

    fn become_leader(&mut self) {
        let last_index = self.log.last_index();
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();
        for &peer in &self.state.peers {
            next_index.insert(peer, last_index + 1);
            match_index.insert(peer, 0);
        }
        self.state.role = Role::Leader { next_index, match_index };
        self.state.heartbeat_elapsed = 0;
        self.broadcast_append_entries();
    }

    fn handle_request_vote(&mut self, from: NodeId, req: RequestVote) {
        let mut grant = false;

        if req.term == self.state.current_term {
            let havent_voted = match self.state.voted_for {
                None => true,
                Some(x) => x == req.candidate_id,
            };
            let candidate_log_ok = req.last_log_term > self.log.last_term()
                || (req.last_log_term == self.log.last_term()
                    && req.last_log_index >= self.log.last_index());

            if havent_voted && candidate_log_ok {
                grant = true;
                self.state.voted_for = Some(req.candidate_id);
                self.state.election_elapsed = 0;
                self.jitter_timeout();
            }
        }

        self.send(
            from,
            Message::RequestVoteReply(RequestVoteReply { term: self.state.current_term, vote_granted: grant }),
        );
    }

    fn handle_request_vote_reply(&mut self, from: NodeId, reply: RequestVoteReply) {
        if reply.term != self.state.current_term || !reply.vote_granted {
            return;
        }

        let should_become_leader = if let Role::Candidate { votes_received } = &mut self.state.role {
            if !votes_received.contains(&from) {
                votes_received.push(from);
            }
            votes_received.len() >= self.state.quorum_size()
        } else {
            false
        };

        if should_become_leader {
            self.become_leader();
        }
    }

    fn handle_append_entries(&mut self, from: NodeId, req: AppendEntries) {
        if req.term < self.state.current_term {
            self.send(
                from,
                Message::AppendEntriesReply(AppendEntriesReply {
                    term: self.state.current_term,
                    success: false,
                    conflict_index: None,
                    match_index: 0,
                }),
            );
            return;
        }

        self.state.election_elapsed = 0;
        self.jitter_timeout();
        if !matches!(self.state.role, Role::Follower) {
            self.state.role = Role::Follower;
        }

        let log_ok = req.prev_log_index == 0
            || self.log.term_at(req.prev_log_index) == Some(req.prev_log_term);

        if !log_ok {
            self.send(
                from,
                Message::AppendEntriesReply(AppendEntriesReply {
                    term: self.state.current_term,
                    success: false,
                    conflict_index: Some(self.log.last_index().min(req.prev_log_index)),
                    match_index: 0,
                }),
            );
            return;
        }

        let mut next_index = req.prev_log_index + 1;
        for entry in req.entries {
            if self.log.term_at(next_index) != Some(entry.term) {
                self.log.truncate_from(next_index);
                self.log.append(entry.term, entry.data);
            }
            next_index += 1;
        }
        // The highest index this follower now actually holds, matching
        // what it just applied -- this is what the leader must trust.
        let replicated_index = next_index - 1;

        if req.leader_commit > self.state.commit_index {
            self.state.commit_index = req.leader_commit.min(self.log.last_index());
            self.apply_committed();
        }

        self.send(
            from,
            Message::AppendEntriesReply(AppendEntriesReply {
                term: self.state.current_term,
                success: true,
                conflict_index: None,
                match_index: replicated_index,
            }),
        );
    }

    fn handle_append_entries_reply(&mut self, from: NodeId, reply: AppendEntriesReply) {
        if reply.term != self.state.current_term {
            return;
        }

        let last_index = self.log.last_index();
        let quorum = self.state.quorum_size();
        let mut newly_committed = None;

        if let Role::Leader { next_index, match_index } = &mut self.state.role {
            if reply.success {
                let current = match_index.get(&from).copied().unwrap_or(0);
                // Never regress: an older, delayed reply must not undo
                // progress recorded by a newer one.
                if reply.match_index > current {
                    match_index.insert(from, reply.match_index);
                }
                next_index.insert(from, reply.match_index + 1);

                let mut indices: Vec<u64> = match_index.values().copied().collect();
                indices.push(last_index);
                indices.sort_unstable_by(|a, b| b.cmp(a));
                let candidate_commit = indices[quorum - 1];
                newly_committed = Some(candidate_commit);
            } else {
                let retry_from = reply.conflict_index.unwrap_or(1).max(1);
                next_index.insert(from, retry_from);
            }
        }

        if let Some(candidate_commit) = newly_committed {
            if candidate_commit > self.state.commit_index {
                self.state.commit_index = candidate_commit;
                self.apply_committed();
            }
        }
    }

    fn apply_committed(&mut self) {
        while self.state.last_applied < self.state.commit_index {
            self.state.last_applied += 1;
            if let Some(entry) = self.log.get(self.state.last_applied) {
                self.committed.push((entry.index, entry.data.clone()));
            }
        }
    }

    fn broadcast_append_entries(&mut self) {
        let peers = self.state.peers.clone();
        for peer in peers {
            self.send_append_entries_to(peer);
        }
    }

    fn send_append_entries_to(&mut self, peer: NodeId) {
        let next = if let Role::Leader { next_index, .. } = &self.state.role {
            *next_index.get(&peer).unwrap_or(&1)
        } else {
            return;
        };

        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = self.log.term_at(prev_log_index).unwrap_or(0);
        let entries = self.log.entries_from(next).to_vec();

        self.send(
            peer,
            Message::AppendEntries(AppendEntries {
                term: self.state.current_term,
                leader_id: self.state.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.state.commit_index,
            }),
        );
    }

    pub fn propose(&mut self, data: Vec<u8>) -> Option<LogIndex> {
        if !self.state.role.is_leader() {
            return None;
        }
        let index = self.log.append(self.state.current_term, data);
        self.broadcast_append_entries();
        Some(index)
    }
}
