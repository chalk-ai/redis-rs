use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use arcstr::ArcStr;

use super::interface::{
    ClusterTopology, NodeAvailabilityZoneDiscoveryCache, NodeAvailabilityZoneDiscoveryState,
    ReadCandidates, ReadRoutingStrategy, ReadRoutingStrategyFactory,
};
use crate::cluster_handling::NodeAddress;
use crate::cluster_handling::slot_range_map::SlotRangeMap;

struct ShardRoutingState {
    local_replicas: HashSet<NodeAddress>,
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
pub struct ZonalReadRoutingStrategy {
    availability_zone: ArcStr,
    shared_state: Arc<NodeAvailabilityZoneDiscoveryState>,
    state: Arc<RwLock<SlotStates>>,
}

impl ZonalReadRoutingStrategy {
    /// Creates a zonal read-routing strategy for the caller's availability zone.
    pub fn new(availability_zone: impl Into<ArcStr>) -> Self {
        Self {
            availability_zone: availability_zone.into(),
            shared_state: Arc::new(NodeAvailabilityZoneDiscoveryState::default()),
            state: Arc::new(RwLock::new(SlotStates {
                slots: SlotRangeMap::new(),
            })),
        }
    }

    /// Creates a cloneable zonal strategy handle for sharing discovery state
    /// across multiple clients in the same process.
    ///
    /// Clones of the returned strategy share availability-zone discovery
    /// results, but each connection keeps independent per-slot routing counters.
    pub fn shared(availability_zone: impl Into<ArcStr>) -> Self {
        Self::new(availability_zone)
    }

    /// Returns the caller availability zone this strategy prefers.
    pub fn availability_zone(&self) -> &str {
        &self.availability_zone
    }
}

impl Clone for ZonalReadRoutingStrategy {
    fn clone(&self) -> Self {
        Self {
            availability_zone: self.availability_zone.clone(),
            shared_state: Arc::clone(&self.shared_state),
            state: Arc::new(RwLock::new(SlotStates {
                slots: SlotRangeMap::new(),
            })),
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
            let local_replicas = shard
                .replicas()
                .iter()
                .filter(|replica| {
                    topology
                        .availability_zone_for_node(replica)
                        .is_some_and(|zone| zone == self.availability_zone.as_str())
                })
                .cloned()
                .collect();
            let state = Arc::new(ShardRoutingState {
                local_replicas,
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

            let local_replicas = shard.replicas().iter().filter(|replica| {
                topology
                    .availability_zone_for_node(replica)
                    .is_some_and(|zone| zone == self.availability_zone.as_str())
            });

            let mut local_count = 0;
            for replica in local_replicas {
                local_count += 1;
                nodes.insert(replica.clone());
            }

            if local_count == 0 {
                nodes.extend(shard.replicas().iter().cloned());
            }
        }

        Some(nodes)
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

        if let Some(shard_state) = shard_state {
            let idx = shard_state.counter.fetch_add(1, Ordering::Relaxed);
            let local_len = replicas
                .iter()
                .filter(|replica| shard_state.local_replicas.contains(*replica))
                .count();
            if local_len > 0 {
                let target_idx = idx % local_len;
                return replicas
                    .iter()
                    .filter(|replica| shard_state.local_replicas.contains(*replica))
                    .nth(target_idx)
                    .expect("local replica exists");
            }

            return replicas
                .get(idx % replicas.len().get())
                .expect("non-empty replicas");
        }

        replicas.first()
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

        assert!(!cache.should_log_coverage(NodeAvailabilityZoneCoverage::Complete));
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
}
