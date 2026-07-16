use crate::log::LogEntry;
use crate::types::{LogIndex, NodeId, Term};

/// Sent by a candidate to request votes during an election.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVote {
    pub term: Term,
    pub candidate_id: NodeId,
    pub last_log_index: LogIndex,
    pub last_log_term: Term,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestVoteReply {
    pub term: Term,
    pub vote_granted: bool,
}

/// Sent by the leader to replicate entries and as a heartbeat when empty.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    pub entries: Vec<LogEntry>,
    pub leader_commit: LogIndex,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendEntriesReply {
    pub term: Term,
    pub success: bool,
    /// Hint for the leader to skip failed probing faster on conflict.
    pub conflict_index: Option<LogIndex>,
    /// The log index the follower actually holds as of this reply
    /// (prev_log_index + entries applied). The leader must use this,
    /// never its own current log length, to update match_index --
    /// otherwise a stale/delayed reply can be misread as confirming
    /// entries the follower never received.
    pub match_index: LogIndex,
}

/// Envelope wrapping every message with its sender, so the core
/// never needs to know about sockets, threads, or transports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    RequestVote(RequestVote),
    RequestVoteReply(RequestVoteReply),
    AppendEntries(AppendEntries),
    AppendEntriesReply(AppendEntriesReply),
}

/// An outbound message the core wants delivered to a specific peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub message: Message,
}
