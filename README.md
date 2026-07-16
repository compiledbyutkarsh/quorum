# quorum 🗳️

A Raft consensus library written from scratch in Rust, with a deterministic
network simulator for testing distributed failure scenarios.

## Design

The core (`Node`) has no networking, no threads, and no wall-clock
dependency. It's driven entirely through two calls:

- `tick()` — advance the node's internal clock by one unit
- `step(from, message)` — feed it an inbound RPC

Every outbound message goes into an outbox that the driver drains and
delivers however it wants — over TCP in production, or through an
in-memory scheduler in tests. This separation is what makes deterministic
simulation possible: the same seed always produces the same sequence of
elections, drops, and delays.

## Simulator

`Simulator` wraps a set of nodes with a seeded PRNG that controls message
delivery — drop rate, delay range, and manual network partitions between
specific node pairs. Two runs with the same seed produce byte-identical
outcomes, which makes flaky distributed bugs reproducible instead of
"couldn't repro on my machine."

## What's implemented

- Leader election with randomized timeouts (re-rolled per attempt, so
  identical base timeouts across nodes don't cause a permanent split-vote
  livelock)
- Log replication with conflict detection and truncation
- Majority-based commit indexing
- Partition tolerance — a minority partition can't make progress; the
  majority side keeps electing leaders and committing entries
- Leader crash recovery

## Testing
cargo test

`tests/election.rs` covers baseline election and replication behavior.
`tests/edge_cases.rs` covers harder scenarios: leader crash, identical
timeout livelock resistance, and log reconciliation after a healed
partition.

That last one caught a real bug during development: an isolated leader
could locally "commit" a write it never actually replicated anywhere,
because match tracking used the leader's own log length instead of what
the follower's reply actually confirmed. A stale reply from before the
partition, delivered after healing, was enough to trigger it. Fixed by
having `AppendEntriesReply` carry the follower's actual replicated index
and having the leader trust only that value, never regressing it on
out-of-order replies.

## License

MIT
