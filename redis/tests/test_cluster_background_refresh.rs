#![cfg(feature = "cluster")]
mod support;

use redis::{
    Value,
    cluster::{ClusterClient, ClusterClientBuilder, ClusterConnection, NodeAddress},
    cluster_read_routing::{
        ClusterTopology, RandomReplicaStrategy, ReadCandidates, ReadRoutingStrategy,
        ReadRoutingStrategyFactory, RoundRobinReplicaStrategy, UniformRandom,
    },
    cmd,
};
use std::{
    collections::{BTreeSet, HashSet},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use support::*;

fn ports(connection: &ClusterConnection<MockConnection>) -> BTreeSet<u16> {
    connection
        .connected_node_addresses()
        .iter()
        .map(NodeAddress::port)
        .collect()
}

fn wait_for_ports(connection: &mut ClusterConnection<MockConnection>, expected: &BTreeSet<u16>) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        // A primary-only command cannot hide missing prepared replicas by
        // opening their connections on demand.
        cmd("SET")
            .arg("test")
            .arg("value")
            .exec(connection)
            .unwrap();
        if ports(connection) == *expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected {expected:?}, got {:?}",
            ports(connection)
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn exercise_replica_changes(name: &'static str, builder: ClusterClientBuilder, subset: bool) {
    let replicas = Arc::new(Mutex::new(vec![6380, 6381]));
    let fetches = Arc::new(AtomicUsize::new(0));
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        builder.periodic_topology_refresh(Duration::from_millis(20)),
        name,
        {
            let replicas = replicas.clone();
            let fetches = fetches.clone();
            move |command, port| {
                if contains_slice(command, b"SLOTS") {
                    fetches.fetch_add(1, Ordering::SeqCst);
                    return respond_startup_with_replica_using_config(
                        name,
                        command,
                        Some(vec![MockSlotRange {
                            primary_port: 6379,
                            replica_ports: replicas.lock().unwrap().clone(),
                            slot_range: 0..16383,
                        }]),
                    );
                }
                respond_startup(name, command)?;
                Err(Ok(if contains_slice(command, b"GET") {
                    Value::Int(port.into())
                } else {
                    Value::Okay
                }))
            }
        },
    );
    let initial = ports(&connection);
    let selected: u16 = cmd("GET").arg("test").query(&mut connection).unwrap();
    assert!([6380, 6381].contains(&selected));
    assert_eq!(initial.len(), if subset { 2 } else { 3 });

    // Two background fetches imply at least one unchanged result was installed:
    // another refresh cannot start while the previous result remains pending.
    let target = fetches.load(Ordering::SeqCst) + 2;
    let deadline = Instant::now() + Duration::from_secs(3);
    while fetches.load(Ordering::SeqCst) < target {
        cmd("SET")
            .arg("test")
            .arg("value")
            .exec(&mut connection)
            .unwrap();
        assert_eq!(ports(&connection), initial);
        assert!(Instant::now() < deadline, "periodic refresh did not run");
        std::thread::sleep(Duration::from_millis(2));
    }

    // Removing the selected replica forces a new connection. Adding it back
    // must restore UniformRandom's original affinity, with no failed reads.
    let other = if selected == 6380 { 6381 } else { 6380 };
    *replicas.lock().unwrap() = vec![other];
    wait_for_ports(&mut connection, &[6379, other].into_iter().collect());
    let actual: u16 = cmd("GET").arg("test").query(&mut connection).unwrap();
    assert_eq!(actual, other);
    *replicas.lock().unwrap() = vec![6380, 6381];
    wait_for_ports(&mut connection, &initial);
    if subset {
        let actual: u16 = cmd("GET").arg("test").query(&mut connection).unwrap();
        assert_eq!(actual, selected);
    }
}

struct CountedUniform(Arc<AtomicUsize>);
impl ReadRoutingStrategyFactory for CountedUniform {
    fn create_strategy(&self) -> Box<dyn ReadRoutingStrategy> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Box::new(UniformRandom::new())
    }
}

#[test]
fn uniform_affinity_survives_refresh_and_replica_replacement() {
    let calls = Arc::new(AtomicUsize::new(0));
    let name = "compact_uniform_refresh";
    exercise_replica_changes(
        name,
        ClusterClient::builder([format!("redis://{name}")])
            .read_routing_strategy(CountedUniform(calls.clone())),
        true,
    );
    // MockEnv also creates an async connection when that feature is enabled.
    assert_eq!(
        calls.load(Ordering::SeqCst),
        if cfg!(feature = "cluster-async") {
            2
        } else {
            1
        }
    );
}

#[test]
fn default_eager_strategies_reconcile_replica_membership() {
    let name = "compact_random_refresh";
    exercise_replica_changes(
        name,
        ClusterClient::builder([format!("redis://{name}")])
            .read_routing_strategy(RandomReplicaStrategy),
        false,
    );
    let name = "compact_round_robin_refresh";
    exercise_replica_changes(
        name,
        ClusterClient::builder([format!("redis://{name}")])
            .read_routing_strategy(RoundRobinReplicaStrategy::new()),
        false,
    );
}

// This custom strategy requires the topology callback before its eager query.
struct CachedStrategy {
    eager: RwLock<HashSet<NodeAddress>>,
    cloneable: bool,
}
impl CachedStrategy {
    fn new(cloneable: bool) -> Self {
        Self {
            eager: RwLock::new(HashSet::new()),
            cloneable,
        }
    }
}
impl ReadRoutingStrategyFactory for CachedStrategy {
    fn create_strategy(&self) -> Box<dyn ReadRoutingStrategy> {
        Box::new(Self::new(self.cloneable))
    }
}
impl ReadRoutingStrategy for CachedStrategy {
    fn clone_for_refresh(&self) -> Option<Box<dyn ReadRoutingStrategy>> {
        self.cloneable
            .then(|| Box::new(Self::new(true)) as Box<dyn ReadRoutingStrategy>)
    }
    fn on_topology_changed(&self, topology: ClusterTopology) {
        *self.eager.write().unwrap() = topology
            .shards()
            .flat_map(|shard| {
                std::iter::once(shard.primary().clone()).chain(shard.replicas().first().cloned())
            })
            .collect();
    }
    fn eager_connection_nodes(&self, _: &ClusterTopology) -> Option<HashSet<NodeAddress>> {
        let eager = self.eager.read().unwrap();
        assert!(!eager.is_empty(), "eager query preceded topology callback");
        Some(eager.clone())
    }
    fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
        let replicas = match candidates {
            ReadCandidates::AnyNode(c) => c.replicas(),
            ReadCandidates::ReplicasOnly(c) => c.replicas(),
        };
        let eager = self.eager.read().unwrap();
        replicas
            .iter()
            .find(|node| eager.contains(*node))
            .expect("routing state was changed before installation")
    }
}

#[test]
fn custom_callback_order_is_preserved_with_and_without_cloning() {
    for (name, cloneable) in [
        ("compact_custom_clone", true),
        ("compact_custom_legacy", false),
    ] {
        exercise_replica_changes(
            name,
            ClusterClient::builder([format!("redis://{name}")])
                .read_routing_strategy(CachedStrategy::new(cloneable)),
            true,
        );
    }
}

#[test]
fn background_workers_are_bounded_and_continue_after_failed_connects() {
    const LIMIT: usize = 8;
    const FAILED: u16 = 7000;
    const NODES: std::ops::Range<u16> = 7000..7041;
    let name = "compact_background_workers";
    let added = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let threads = Arc::new(Mutex::new(std::collections::HashMap::new()));
    set_connect_errors(name, [FAILED]);
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder([format!("redis://{name}")])
            .periodic_topology_refresh(Duration::from_millis(20))
            .read_routing_strategy(RoundRobinReplicaStrategy::new()),
        name,
        {
            let added = added.clone();
            let threads = threads.clone();
            move |command, port| {
                if contains_slice(command, b"READONLY") && NODES.contains(&port) {
                    // Preserve each node's first worker; later metadata queries
                    // can also connect to this node from the control thread.
                    threads
                        .lock()
                        .unwrap()
                        .entry(port)
                        .or_insert_with(|| std::thread::current().id());
                }
                if contains_slice(command, b"SLOTS") {
                    return respond_startup_with_replica_using_config(
                        name,
                        command,
                        Some(vec![MockSlotRange {
                            primary_port: 6379,
                            replica_ports: if added.load(Ordering::SeqCst) {
                                NODES.collect()
                            } else {
                                vec![]
                            },
                            slot_range: 0..16383,
                        }]),
                    );
                }
                Err(Ok(Value::Okay))
            }
        },
    );
    added.store(true, Ordering::SeqCst);
    wait_for_ports(
        &mut connection,
        &std::iter::once(6379)
            .chain(NODES.filter(|port| *port != FAILED))
            .collect(),
    );
    let threads = threads.lock().unwrap();
    assert_eq!(
        threads.len(),
        40,
        "a failed connection dropped the rest of its batch"
    );
    assert_eq!(
        threads.values().collect::<HashSet<_>>().len(),
        LIMIT,
        "refresh did not reuse a bounded set of workers"
    );
}
