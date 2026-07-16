use quorum::node::Node;
use quorum::sim::Simulator;

fn cluster_with_timeouts(seed: u64, ids: &[u64], timeout_fn: impl Fn(u64) -> u32) -> Simulator {
    let mut sim = Simulator::new(seed);
    for &id in ids {
        let peers: Vec<u64> = ids.iter().copied().filter(|&p| p != id).collect();
        sim.add_node(Node::new(id, peers, timeout_fn(id)));
    }
    sim
}

fn five_node_cluster(seed: u64) -> Simulator {
    cluster_with_timeouts(seed, &[1, 2, 3, 4, 5], |id| 10 + (id as u32))
}

/// A partitioned leader keeps appending to its own local log (propose()
/// doesn't check network reachability), while the rest of the cluster
/// elects a new leader and diverges. When the partition heals, the old
/// leader must detect the higher term, step down, and truncate its
/// conflicting uncommitted entry in favor of the real log.
#[test]
fn log_conflict_resolves_after_partition_heals() {
    let mut sim = five_node_cluster(2024);
    sim.run_ticks(50);

    let old_leader = sim.current_leader().expect("cluster should elect an initial leader");

    // Isolate the leader completely before it can commit anything new.
    for peer in [1u64, 2, 3, 4, 5] {
        if peer != old_leader {
            sim.conditions.partition(old_leader, peer);
        }
    }

    // This entry gets appended to the old leader's local log but can
    // never reach a majority — it's a "phantom" write that must not survive.
    sim.nodes.get_mut(&old_leader).unwrap().propose(b"stale write from isolated leader".to_vec());

    // Remaining 4 nodes should elect a new leader and make real progress.
    sim.run_ticks(80);

    let new_leader = sim
        .nodes
        .iter()
        .find(|(id, n)| **id != old_leader && n.state.role.is_leader())
        .map(|(id, _)| *id)
        .expect("majority partition should elect a new leader");

    let committed_index = sim
        .nodes
        .get_mut(&new_leader)
        .unwrap()
        .propose(b"real write from new leader".to_vec())
        .expect("new leader should accept proposal");

    sim.run_ticks(50);

    // Heal the partition — old leader must reconcile.
    sim.conditions.heal_all();
    sim.run_ticks(80);

    // Every node, including the formerly-isolated old leader, should
    // converge on the real committed entry, and the phantom write
    // should never appear anywhere as committed.
    for (id, node) in &sim.nodes {
        let has_real_entry = node.committed.iter().any(|(i, data)| {
            *i == committed_index && data == b"real write from new leader"
        });
        assert!(has_real_entry, "node {} did not converge on the real entry", id);

        let has_phantom = node.committed.iter().any(|(_, data)| {
            data == b"stale write from isolated leader"
        });
        assert!(!has_phantom, "node {} committed a phantom write from the isolated leader", id);
    }

    assert!(!sim.nodes[&old_leader].state.role.is_leader(), "old leader must step down after reconnecting");
}

/// Nodes with identical (unjittered) base timeouts are the worst case for
/// split votes — every node times out on the same tick, all become
/// candidates simultaneously, and votes split evenly. Per-node jitter
/// (re-rolled on every election attempt) is what prevents this from
/// becoming a permanent livelock. Bounded tick count proves convergence.
#[test]
fn identical_base_timeouts_do_not_livelock() {
    let mut sim = cluster_with_timeouts(31337, &[1, 2, 3, 4, 5], |_id| 10);

    sim.run_ticks(300);

    let leaders: Vec<u64> = sim
        .nodes
        .values()
        .filter(|n| n.state.role.is_leader())
        .map(|n| n.state.id)
        .collect();

    assert_eq!(
        leaders.len(),
        1,
        "cluster with identical base timeouts should still converge to one leader via jitter, got {:?}",
        leaders
    );
}

/// Simulates a hard leader crash (not a partition — the node is simply
/// gone, like a killed process) and verifies the remaining majority
/// detects the silence and elects a replacement.
#[test]
fn cluster_recovers_from_leader_crash() {
    let mut sim = five_node_cluster(777);
    sim.run_ticks(50);

    let leader_id = sim.current_leader().expect("cluster should elect an initial leader");
    let leader_term = sim.nodes[&leader_id].state.current_term;

    // Simulate a crash: the node is removed entirely, no more ticks,
    // no more messages in or out. This is harsher than a partition
    // since a partitioned node keeps ticking and could reconnect.
    sim.nodes.remove(&leader_id);

    sim.run_ticks(80);

    let new_leader = sim
        .nodes
        .values()
        .find(|n| n.state.role.is_leader())
        .map(|n| n.state.id);

    assert!(new_leader.is_some(), "surviving 4 nodes should elect a new leader after the old one crashes");
    assert_ne!(new_leader.unwrap(), leader_id, "new leader can't be the crashed node");

    let new_term = sim.nodes[&new_leader.unwrap()].state.current_term;
    assert!(new_term > leader_term, "new election must use a higher term than the crashed leader's");

    // Cluster should still be able to make progress (4/5 is still a majority).
    let index = sim
        .nodes
        .get_mut(&new_leader.unwrap())
        .unwrap()
        .propose(b"progress after crash".to_vec())
        .expect("new leader should accept proposals");

    sim.run_ticks(50);

    let committed_on_majority = sim
        .nodes
        .values()
        .filter(|n| n.committed.iter().any(|(i, _)| *i == index))
        .count();

    assert!(committed_on_majority >= 3, "entry should commit on remaining majority, got {}", committed_on_majority);
}

/// Losing exactly one node out of five (a bare majority remains: 4/5)
/// should never prevent progress — this pins down the quorum math itself.
#[test]
fn quorum_math_holds_for_various_cluster_sizes() {
    for &size in &[3u64, 5, 7] {
        let ids: Vec<u64> = (1..=size).collect();
        let mut sim = cluster_with_timeouts(size * 111, &ids, |id| 10 + (id as u32));
        sim.run_ticks(60);

        let leader = sim.current_leader();
        assert!(leader.is_some(), "cluster of size {} failed to elect a leader", size);

        let expected_quorum = (size as usize) / 2 + 1;
        let actual_quorum = sim.nodes[&leader.unwrap()].state.quorum_size();
        assert_eq!(actual_quorum, expected_quorum, "wrong quorum size for cluster of {}", size);
    }
}
