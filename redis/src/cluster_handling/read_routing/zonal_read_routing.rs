use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use arcstr::ArcStr;

use super::circuit_breaker::{ReadCircuitBreaker, ReadCircuitBreakerConfig, ReadOutcome};
use super::interface::{
    ClusterTopology, NodeAvailabilityZoneDiscoveryCache, NodeAvailabilityZoneDiscoveryState,
    ReadCandidates, ReadRoutingStrategy, ReadRoutingStrategyFactory, Replicas,
};
use crate::cluster_handling::NodeAddress;
use crate::cluster_handling::slot_range_map::SlotRangeMap;

struct ShardRoutingState {
    local_replica_indices: Vec<usize>,
    remote_replica_indices: Vec<usize>,
    counter: AtomicUsize,
}

struct SlotStates {
    slots: SlotRangeMap<Arc<ShardRoutingState>>,
}

/// Routes reads to replicas in the caller's availability zone first.
///
/// For each shard, reads are routed round-robin across replicas whose
/// discovered availability zone matches the caller-provided zone. If a shard
/// has no same-zone replicas, reads fall back to all replicas for that shard.
/// If a shard has no replicas at all, the cluster connection falls back to the
/// primary without invoking the strategy.
///
/// During topology refresh this strategy asks the connection to discover node
/// availability zones and to eagerly connect only to nodes it may route to:
/// all primaries, same-zone replicas when present, and otherwise all replicas
/// for shards with no same-zone replica.
///
/// # Circuit breaker
///
/// By default the strategy keeps a [`ReadCircuitBreaker`], shared by every
/// clone, that skips replicas which answered `LOADING` or `MASTERDOWN` or
/// failed several reads in a row. When every same-zone replica of a shard is
/// open, reads go round-robin to the shard's other-zone replicas, then to the
/// primary for reads that allow it. A replica-only read with every replica
/// open still goes to its usual replica. After the breaker's open duration
/// one probe read is sent to the skipped replica, and a reply closes the
/// breaker.
pub struct ZonalReadRoutingStrategy {
    availability_zone: ArcStr,
    shared_state: Arc<NodeAvailabilityZoneDiscoveryState>,
    state: Arc<RwLock<SlotStates>>,
    circuit_breaker: Option<ReadCircuitBreaker>,
}

impl ZonalReadRoutingStrategy {
    /// Creates a zonal read-routing strategy for the caller's availability zone
    /// with the default [`ReadCircuitBreakerConfig`].
    pub fn new(availability_zone: impl Into<ArcStr>) -> Self {
        Self {
            availability_zone: availability_zone.into(),
            shared_state: Arc::new(NodeAvailabilityZoneDiscoveryState::default()),
            state: Arc::new(RwLock::new(SlotStates {
                slots: SlotRangeMap::new(),
            })),
            circuit_breaker: Some(ReadCircuitBreaker::default()),
        }
    }

    /// Creates a cloneable zonal strategy handle for sharing discovery state
    /// across multiple clients in the same process.
    ///
    /// Clones of the returned strategy share availability-zone discovery
    /// results and the circuit breaker, but each connection keeps independent
    /// per-slot routing counters.
    pub fn shared(availability_zone: impl Into<ArcStr>) -> Self {
        Self::new(availability_zone)
    }

    /// Replaces the circuit breaker with a new one using `config`.
    pub fn with_circuit_breaker(mut self, config: ReadCircuitBreakerConfig) -> Self {
        self.circuit_breaker = Some(ReadCircuitBreaker::new(config));
        self
    }

    /// Disables the circuit breaker, so reads always go to the zone-preferred
    /// replica.
    pub fn without_circuit_breaker(mut self) -> Self {
        self.circuit_breaker = None;
        self
    }

    /// Returns the circuit breaker shared by this strategy's clones, if enabled.
    pub fn circuit_breaker(&self) -> Option<&ReadCircuitBreaker> {
        self.circuit_breaker.as_ref()
    }

    /// Returns the caller availability zone this strategy prefers.
    pub fn availability_zone(&self) -> &str {
        &self.availability_zone
    }

    fn is_local(&self, topology: &ClusterTopology, node: &NodeAddress) -> bool {
        topology
            .availability_zone_for_node(node)
            .is_some_and(|zone| zone == self.availability_zone.as_str())
    }
}

/// Returns the first replica, in round-robin order from `start`, that the
/// breaker lets through. Probes are granted as a side effect, so the scan
/// stops at the first available replica.
fn first_available<'a>(
    replicas: &Replicas<'a>,
    count: usize,
    index_at: impl Fn(usize) -> usize,
    start: usize,
    breaker: &ReadCircuitBreaker,
) -> Option<&'a NodeAddress> {
    (0..count)
        .filter_map(|offset| replicas.get(index_at((start + offset) % count)))
        .find(|replica| breaker.is_available(replica))
}

impl Clone for ZonalReadRoutingStrategy {
    fn clone(&self) -> Self {
        Self {
            availability_zone: self.availability_zone.clone(),
            shared_state: Arc::clone(&self.shared_state),
            state: Arc::new(RwLock::new(SlotStates {
                slots: SlotRangeMap::new(),
            })),
            circuit_breaker: self.circuit_breaker.clone(),
        }
    }
}

impl ReadRoutingStrategyFactory for ZonalReadRoutingStrategy {
    fn create_strategy(&self) -> Box<dyn ReadRoutingStrategy> {
        Box::new(self.clone())
    }
}

impl ReadRoutingStrategy for ZonalReadRoutingStrategy {
    fn node_availability_zone_discovery_cache(
        &self,
    ) -> Option<Arc<dyn NodeAvailabilityZoneDiscoveryCache>> {
        Some(self.shared_state.clone())
    }

    fn on_topology_changed(&self, topology: ClusterTopology) {
        let mut slots = SlotRangeMap::new();
        for shard in topology.shards() {
            let (local_replica_indices, remote_replica_indices) = (0..shard.replicas().len())
                .partition(|&idx| self.is_local(&topology, &shard.replicas()[idx]));
            let state = Arc::new(ShardRoutingState {
                local_replica_indices,
                remote_replica_indices,
                counter: AtomicUsize::new(0),
            });
            for &(start, end) in shard.slot_ranges() {
                slots.insert(start, end, Arc::clone(&state));
            }
        }

        let mut state = self.state.write().expect("Lock poisoned");
        state.slots = slots;
    }

    fn eager_connection_nodes(&self, topology: &ClusterTopology) -> Option<HashSet<NodeAddress>> {
        let mut nodes = HashSet::new();

        for shard in topology.shards() {
            nodes.insert(shard.primary().clone());

            let local_replicas: Vec<_> = shard
                .replicas()
                .iter()
                .filter(|replica| self.is_local(topology, replica))
                .collect();
            nodes.extend(local_replicas.iter().map(|replica| (*replica).clone()));

            // Keep the other-zone replicas connected while every local one is
            // open, so a topology refresh does not prune the connections reads
            // are falling back to.
            let local_all_open = self.circuit_breaker.as_ref().is_some_and(|breaker| {
                local_replicas
                    .iter()
                    .all(|replica| breaker.is_open(replica))
            });
            if local_replicas.is_empty() || local_all_open {
                nodes.extend(shard.replicas().iter().cloned());
            }
        }

        Some(nodes)
    }

    fn tracks_node_health(&self) -> bool {
        self.circuit_breaker.is_some()
    }

    fn on_read_outcome(&self, node: &NodeAddress, outcome: ReadOutcome) {
        if let Some(breaker) = &self.circuit_breaker {
            breaker.record(node, outcome);
        }
    }

    fn route_read<'a>(&self, candidates: &ReadCandidates<'a>) -> &'a NodeAddress {
        let replicas = match candidates {
            ReadCandidates::AnyNode(c) => c.replicas(),
            ReadCandidates::ReplicasOnly(c) => c.replicas(),
        };

        let shard_state = {
            let state = self.state.read().expect("Lock poisoned");
            state.slots.get(candidates.slot()).cloned()
        };

        let Some(shard_state) = shard_state else {
            return replicas.first();
        };

        let idx = shard_state.counter.fetch_add(1, Ordering::Relaxed);
        let local = &shard_state.local_replica_indices;
        let remote = &shard_state.remote_replica_indices;

        if let Some(breaker) = &self.circuit_breaker {
            let available = if local.is_empty() {
                first_available(replicas, replicas.len().get(), |i| i, idx, breaker)
            } else {
                first_available(replicas, local.len(), |i| local[i], idx, breaker).or_else(|| {
                    first_available(replicas, remote.len(), |i| remote[i], idx, breaker)
                })
            };
            if let Some(replica) = available {
                return replica;
            }
            if let ReadCandidates::AnyNode(c) = candidates {
                return c.primary();
            }
        }

        if !local.is_empty() {
            if let Some(replica) = replicas.get(local[idx % local.len()]) {
                return replica;
            }
        }

        replicas
            .get(idx % replicas.len().get())
            .expect("non-empty replicas")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    use crate::cluster_handling::read_routing::{
        NodeAvailabilityZoneCoverage, NodeAvailabilityZoneDiscoveryMethod, Replicas, Shard,
    };

    fn node(host: &str, port: u16) -> NodeAddress {
        NodeAddress::from_parts(host.into(), port)
    }

    fn topology() -> ClusterTopology {
        ClusterTopology::from_shards_with_node_availability_zones(
            vec![
                Shard::new(
                    vec![(0, 1000)],
                    node("primary1", 6379),
                    vec![
                        node("replica1a", 6379),
                        node("replica1b", 6379),
                        node("replica1c", 6379),
                    ],
                ),
                Shard::new(
                    vec![(1001, 2000)],
                    node("primary2", 6379),
                    vec![node("replica2a", 6379), node("replica2b", 6379)],
                ),
            ],
            [
                (node("replica1a", 6379), "us-east-1b".into()),
                (node("replica1b", 6379), "us-east-1c".into()),
                (node("replica1c", 6379), "us-east-1b".into()),
                (node("replica2a", 6379), "us-east-1c".into()),
                (node("replica2b", 6379), "us-east-1d".into()),
            ]
            .into_iter()
            .collect(),
        )
    }

    #[test]
    fn routes_to_local_replicas_first() {
        let strategy = ZonalReadRoutingStrategy::new("us-east-1b");
        strategy.on_topology_changed(topology());

        let replicas = [
            node("replica1a", 6379),
            node("replica1b", 6379),
            node("replica1c", 6379),
        ];
        let candidates = ReadCandidates::replicas_only(1, Replicas::new(&replicas).unwrap());

        assert_eq!(strategy.route_read(&candidates), &replicas[0]);
        assert_eq!(strategy.route_read(&candidates), &replicas[2]);
        assert_eq!(strategy.route_read(&candidates), &replicas[0]);
    }

    #[test]
    fn falls_back_to_all_replicas_when_shard_has_no_local_replicas() {
        let strategy = ZonalReadRoutingStrategy::new("us-east-1b");
        strategy.on_topology_changed(topology());

        let replicas = [node("replica2a", 6379), node("replica2b", 6379)];
        let candidates = ReadCandidates::replicas_only(1500, Replicas::new(&replicas).unwrap());

        assert_eq!(strategy.route_read(&candidates), &replicas[0]);
        assert_eq!(strategy.route_read(&candidates), &replicas[1]);
        assert_eq!(strategy.route_read(&candidates), &replicas[0]);
    }

    #[test]
    fn eager_connections_include_primaries_local_replicas_and_remote_fallbacks() {
        let strategy = ZonalReadRoutingStrategy::new("us-east-1b");
        let nodes = strategy.eager_connection_nodes(&topology()).unwrap();

        assert!(nodes.contains(&node("primary1", 6379)));
        assert!(nodes.contains(&node("primary2", 6379)));
        assert!(nodes.contains(&node("replica1a", 6379)));
        assert!(nodes.contains(&node("replica1c", 6379)));
        assert!(!nodes.contains(&node("replica1b", 6379)));
        assert!(nodes.contains(&node("replica2a", 6379)));
        assert!(nodes.contains(&node("replica2b", 6379)));
    }

    #[test]
    fn shared_discovery_state_logs_only_important_coverage_transitions() {
        let cache = NodeAvailabilityZoneDiscoveryState::default();

        assert!(cache.should_log_coverage(NodeAvailabilityZoneCoverage::Complete));
        assert!(!cache.should_log_coverage(NodeAvailabilityZoneCoverage::Complete));
        assert!(cache.should_log_coverage(NodeAvailabilityZoneCoverage::None));
        assert!(!cache.should_log_coverage(NodeAvailabilityZoneCoverage::None));
        assert!(cache.should_log_coverage(NodeAvailabilityZoneCoverage::Partial));
        assert!(!cache.should_log_coverage(NodeAvailabilityZoneCoverage::Partial));
        assert!(cache.should_log_coverage(NodeAvailabilityZoneCoverage::Complete));
    }

    #[test]
    fn cloned_strategies_share_discovered_zones_but_not_slot_counters() {
        let strategy = ZonalReadRoutingStrategy::shared("us-east-1b");
        let clone = strategy.clone();
        let cached_node = node("replica1a", 6379);
        let mut zones = HashMap::new();
        zones.insert(cached_node.clone(), ArcStr::from("us-east-1b"));

        strategy
            .node_availability_zone_discovery_cache()
            .unwrap()
            .record_success(NodeAvailabilityZoneDiscoveryMethod::ClusterShards, &zones);

        assert_eq!(
            clone
                .node_availability_zone_discovery_cache()
                .unwrap()
                .cached_zones(std::slice::from_ref(&cached_node))
                .get(&cached_node),
            Some(&ArcStr::from("us-east-1b"))
        );

        strategy.on_topology_changed(topology());
        clone.on_topology_changed(topology());
        let replicas = [
            node("replica1a", 6379),
            node("replica1b", 6379),
            node("replica1c", 6379),
        ];
        let candidates = ReadCandidates::replicas_only(1, Replicas::new(&replicas).unwrap());

        assert_eq!(strategy.route_read(&candidates), &replicas[0]);
        assert_eq!(strategy.route_read(&candidates), &replicas[2]);
        assert_eq!(clone.route_read(&candidates), &replicas[0]);
    }

    fn shard1_replicas() -> [NodeAddress; 3] {
        [
            node("replica1a", 6379),
            node("replica1b", 6379),
            node("replica1c", 6379),
        ]
    }

    #[test]
    fn breaker_skips_open_local_replica_for_other_local_replica() {
        let strategy = ZonalReadRoutingStrategy::new("us-east-1b");
        strategy.on_topology_changed(topology());
        let replicas = shard1_replicas();
        let candidates = ReadCandidates::replicas_only(1, Replicas::new(&replicas).unwrap());

        strategy.on_read_outcome(&replicas[0], ReadOutcome::Unavailable);

        for _ in 0..4 {
            assert_eq!(strategy.route_read(&candidates), &replicas[2]);
        }
    }

    #[test]
    fn breaker_falls_back_to_other_zone_replicas_then_primary() {
        let strategy = ZonalReadRoutingStrategy::new("us-east-1b");
        strategy.on_topology_changed(topology());
        let replicas = shard1_replicas();
        let primary = node("primary1", 6379);

        strategy.on_read_outcome(&replicas[0], ReadOutcome::Unavailable);
        strategy.on_read_outcome(&replicas[2], ReadOutcome::Unavailable);

        let any_node = ReadCandidates::any_node(1, &primary, Replicas::new(&replicas).unwrap());
        assert_eq!(strategy.route_read(&any_node), &replicas[1]);

        strategy.on_read_outcome(&replicas[1], ReadOutcome::Unavailable);
        assert_eq!(strategy.route_read(&any_node), &primary);

        // A replica-only read keeps its zone preference when nothing is up.
        let replicas_only = ReadCandidates::replicas_only(1, Replicas::new(&replicas).unwrap());
        let chosen = strategy.route_read(&replicas_only);
        assert!(chosen == &replicas[0] || chosen == &replicas[2]);
    }

    #[test]
    fn breaker_is_shared_by_clones_and_can_be_disabled() {
        let strategy = ZonalReadRoutingStrategy::shared("us-east-1b");
        let clone = strategy.clone();
        clone.on_topology_changed(topology());
        let replicas = shard1_replicas();
        let candidates = ReadCandidates::replicas_only(1, Replicas::new(&replicas).unwrap());

        strategy.on_read_outcome(&replicas[0], ReadOutcome::Unavailable);
        assert_eq!(clone.route_read(&candidates), &replicas[2]);
        assert_eq!(clone.route_read(&candidates), &replicas[2]);

        let disabled = ZonalReadRoutingStrategy::new("us-east-1b").without_circuit_breaker();
        assert!(!disabled.tracks_node_health());
        disabled.on_topology_changed(topology());
        disabled.on_read_outcome(&replicas[0], ReadOutcome::Unavailable);
        assert_eq!(disabled.route_read(&candidates), &replicas[0]);
    }

    #[test]
    fn eager_connections_add_other_zone_replicas_while_local_replicas_are_open() {
        let strategy = ZonalReadRoutingStrategy::new("us-east-1b");
        let replicas = shard1_replicas();

        strategy.on_read_outcome(&replicas[0], ReadOutcome::Unavailable);
        let nodes = strategy.eager_connection_nodes(&topology()).unwrap();
        assert!(!nodes.contains(&replicas[1]));

        strategy.on_read_outcome(&replicas[2], ReadOutcome::Unavailable);
        let nodes = strategy.eager_connection_nodes(&topology()).unwrap();
        assert!(nodes.contains(&replicas[1]));
    }
}
