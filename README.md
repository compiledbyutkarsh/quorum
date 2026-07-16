# quorum

A Raft consensus library implemented from scratch in Rust, built around a deterministic network simulator for testing distributed failure scenarios.

## Overview

Consensus algorithms are difficult to get right, and even harder to test correctly. Most bugs only surface under specific interleavings of message delays, drops, and partitions that are nearly impossible to reproduce with real networking. This project separates the Raft state machine entirely from I/O, so the exact same core code can be driven either by a real transport in production or by a fully controlled, seeded simulator in tests.

## Architecture

The core type, Node, has no knowledge of sockets, threads, or wall-clock time. It exposes exactly two entry points:

- tick() advances the node's internal logical clock by one unit
- step(from, message) delivers an inbound RPC to the node

Every outbound message produced by a call to tick() or step() is placed in an outbox, which the driver is responsible for draining and delivering. This inversion of control means the consensus core is completely agnostic to how messages actually travel, whether over TCP in production or through an in-memory scheduler in tests, and is the foundation that makes deterministic simulation possible.

## Simulator

Simulator owns a set of nodes and a seeded pseudo-random number generator that controls every non-deterministic aspect of message delivery: drop rate, delay range, and manual partitions between specific node pairs. Two simulation runs given the same seed produce byte-identical outcomes, the same elections, the same drops, the same final state, which turns an intermittent CI failure into a reproducible, debuggable test case.

## Implemented

- Leader election with randomized timeouts, re-rolled on every election attempt, so nodes sharing an identical base timeout do not deadlock in a repeated split vote
- Log replication with conflict detection and truncation on divergent entries
- Majority-based commit indexing
- Partition tolerance: a minority partition cannot make progress, while the majority side continues to elect leaders and commit entries
- Leader crash recovery

## Testing

Run the test suite with cargo test.

tests/election.rs covers baseline election and replication behavior. tests/edge_cases.rs covers harder scenarios: leader crash recovery, resistance to split-vote livelock under identical timeouts, and log reconciliation after a healed network partition.

### A bug the simulator caught

The partition-heal test surfaced a real correctness bug during development: an isolated leader could locally mark a write as committed without it ever being replicated to any other node. The cause was that match-index tracking used the leader's own log length rather than the index a follower's reply actually confirmed. A stale reply sent before the partition, delivered only after it healed, was enough to trigger it.

The fix was to have AppendEntriesReply carry the follower's actual replicated index explicitly, and to have the leader trust only that value when advancing match_index, never regressing it on delayed or out-of-order replies.

## License

MIT
