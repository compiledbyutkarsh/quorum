# Quorum 🗳️

A Raft implementation I built from scratch in Rust, mostly to actually understand consensus instead of just reading about it. Comes with a deterministic simulator so I could throw dropped packets, delays, and network partitions at it without needing five terminals open.

## Why deterministic simulation 🧪

Distributed systems bugs are miserable to debug because they usually only show up under one specific ordering of events, and that ordering never repeats. So instead of testing over real sockets, the whole thing runs on a fake clock. A driver calls `tick()` and `step()` on each node manually, and a seeded RNG decides which messages get dropped, delayed, or blocked by a partition. Same seed, same run, every time. If something breaks, it breaks the same way twice.

## How it's structured 🏗️

The `Node` itself doesn't know what a socket is. It has two entry points:

```rust
node.tick();                    // advance its internal clock by one step
node.step(from_id, message);    // hand it an incoming RPC
```

Anything it wants to send back goes into an outbox, and it's on the caller to actually deliver it — over the network for real usage, or straight into another node's `step()` in the simulator. That's the whole trick: the consensus logic has zero opinion about how bytes move around.

## What's actually working ✅

- Leader election, with timeouts that get re-rolled after every failed attempt (without this, nodes with the same base timeout can split-vote forever — found that out the hard way)
- Log replication, including truncating conflicting entries when a follower's log disagrees with the leader's
- Commit indexing based on majority acknowledgement
- Partitions — a minority side just can't make progress, majority side keeps going
- Recovering cleanly when a leader dies

## Running tests 🧵cargo test

`tests/election.rs` is the boring stuff — does it elect a leader, does it replicate. `tests/edge_cases.rs` is where it gets more interesting: leader crashes mid-cluster, nodes with identical timeouts, and reconciling logs after a partition heals.

That last one actually caught a real bug 🐛. An isolated leader was locally marking a write as committed even though it had zero confirmation any other node had it. Turned out the leader was tracking replication progress using its own log length instead of what the follower's reply actually said it had received — so a delayed reply from before the partition was enough to trick it into thinking it had quorum. Fixed by making the follower explicitly report back the index it actually applied, and having the leader only ever move that number forward, never backward.
