use crate::types::{NodeId, Term};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Follower,
    Candidate {
        /// Votes received so far this term, including our own.
        votes_received: Vec<NodeId>,
    },
    Leader {
        /// Next log index to send to each peer.
        next_index: std::collections::HashMap<NodeId, u64>,
        /// Highest log index known replicated on each peer.
        match_index: std::collections::HashMap<NodeId, u64>,
    },
}

impl Role {
    pub fn is_leader(&self) -> bool {
        matches!(self, Role::Leader { .. })
    }
}

/// Persistent + volatile state a single Raft node tracks.
/// Deliberately holds no socket, no thread, no clock — those live
/// outside the core and drive it via `tick()` / `step()`.
#[derive(Debug)]
pub struct NodeState {
    pub id: NodeId,
    pub peers: Vec<NodeId>,

    // --- Persistent state (must survive restarts in a real deployment) ---
    pub current_term: Term,
    pub voted_for: Option<NodeId>,

    // --- Volatile state ---
    pub role: Role,
    pub commit_index: u64,
    pub last_applied: u64,

    /// Ticks elapsed since we last heard from a leader or granted a vote.
    /// Compared against a randomized election timeout owned by the driver.
    pub election_elapsed: u32,
    pub heartbeat_elapsed: u32,
}

impl NodeState {
    pub fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        NodeState {
            id,
            peers,
            current_term: 0,
            voted_for: None,
            role: Role::Follower,
            commit_index: 0,
            last_applied: 0,
            election_elapsed: 0,
            heartbeat_elapsed: 0,
        }
    }

    /// Cluster size including self — used for majority calculations.
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    pub fn quorum_size(&self) -> usize {
        self.cluster_size() / 2 + 1
    }
}
