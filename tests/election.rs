use quorum::node::Node;
use quorum::sim::Simulator;

fn five_node_cluster(seed: u64) -> Simulator {
    let ids = [1, 2, 3, 4, 5];
    let mut sim = Simulator::new(seed);

    for &id in &ids {
        let peers: Vec<u64> = ids.iter().copied().filter(|&p| p != id).collect();
        // Stagger election timeouts slightly (deterministically, via id)
        // so votes don't all fire on the same tick and cause repeated splits.
        let timeout = 10 + (id as u32);
        sim.add_node(Node::new(id, peers, timeout));
    }

    sim
}

#[test]
fn elects_a_single_leader() {
    let mut sim = five_node_cluster(42);

    sim.run_ticks(50);

    let leaders: Vec<u64> = sim
        .nodes
        .values()
        .filter(|n| n.state.role.is_leader())
        .map(|n| n.state.id)
        .collect();

    assert_eq!(leaders.len(), 1, "expected exactly one leader, got {:?}", leaders);
}

#[test]
fn same_seed_produces_identical_outcome() {
    let mut sim_a = five_node_cluster(7);
    let mut sim_b = five_node_cluster(7);

    sim_a.run_ticks(50);
    sim_b.run_ticks(50);

    assert_eq!(sim_a.current_leader(), sim_b.current_leader());
    assert_eq!(sim_a.nodes[&1].state.current_term, sim_b.nodes[&1].state.current_term);
}

#[test]
fn committed_entry_replicates_to_majority() {
    let mut sim = five_node_cluster(99);
    sim.run_ticks(50);

    let leader_id = sim.current_leader().expect("cluster should have elected a leader");

    let index = sim
        .nodes
        .get_mut(&leader_id)
        .unwrap()
        .propose(b"set x = 1".to_vec())
        .expect("leader should accept proposal");

    sim.run_ticks(20);

    let committed_count = sim
        .nodes
        .values()
        .filter(|n| n.committed.iter().any(|(i, _)| *i == index))
        .count();

    assert!(committed_count >= 3, "entry should reach a majority (3/5), got {}", committed_count);
}

#[test]
fn survives_minority_partition() {
    let mut sim = five_node_cluster(123);
    sim.run_ticks(50);

    let leader_before = sim.current_leader().expect("should have a leader before partition");

    // Partition 2 of 5 nodes away from the rest — minority, so the
    // remaining majority should keep functioning without them.
    let isolated = [4u64, 5u64];
    for &a in &isolated {
        for b in [1u64, 2, 3, 4, 5] {
            if a != b {
                sim.conditions.partition(a, b);
            }
        }
    }

    sim.run_ticks(50);

    let majority_leader = sim
        .nodes
        .iter()
        .filter(|(id, _)| !isolated.contains(id))
        .find(|(_, n)| n.state.role.is_leader())
        .map(|(id, _)| *id);

    assert!(majority_leader.is_some(), "majority partition should retain or elect a leader");
    let _ = leader_before; // may or may not still be leader; only majority-liveness matters

    sim.conditions.heal_all();
    sim.run_ticks(50);

    let leaders_after_heal: Vec<u64> = sim
        .nodes
        .values()
        .filter(|n| n.state.role.is_leader())
        .map(|n| n.state.id)
        .collect();

    assert_eq!(leaders_after_heal.len(), 1, "cluster should reconverge to one leader after heal");
}

#[test]
fn lossy_network_still_converges() {
    let mut sim = five_node_cluster(555);
    sim.conditions.drop_rate_pct = 20;
    sim.conditions.min_delay = 1;
    sim.conditions.max_delay = 3;

    sim.run_ticks(200);

    assert!(sim.current_leader().is_some(), "cluster should elect a leader even with 20% packet loss");
}
