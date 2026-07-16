//! Fundamental identifiers used throughout the consensus core.

/// Unique identifier for a node in the cluster.
pub type NodeId = u64;

/// Monotonically increasing election term.
pub type Term = u64;

/// 1-indexed position in the replicated log.
/// Index 0 is reserved as a sentinel for "no entry".
pub type LogIndex = u64;
