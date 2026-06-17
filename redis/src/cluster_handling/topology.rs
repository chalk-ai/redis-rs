//! This module provides the functionality to refresh and calculate the cluster topology for Redis Cluster.

use std::collections::{HashMap, HashSet};

use arcstr::ArcStr;

use super::NodeAddress;
use super::slot_map::SlotRange;
use crate::{RedisResult, Value, connection::is_wildcard_address};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeAvailabilityZoneDiscoveryMethod {
    ClusterShards,
    InfoServer,
    Hostname,
}

impl NodeAvailabilityZoneDiscoveryMethod {
    pub(crate) const ALL: [Self; 3] = [Self::ClusterShards, Self::Hostname, Self::InfoServer];
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeAvailabilityZoneCoverage {
    None,
    Partial,
    Complete,
}

impl NodeAvailabilityZoneCoverage {
    pub(crate) fn from_counts(known_nodes: usize, total_nodes: usize) -> Self {
        if known_nodes == 0 {
            Self::None
        } else if known_nodes >= total_nodes {
            Self::Complete
        } else {
            Self::Partial
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct NodeAvailabilityZoneDiscovery {
    preferred_method: Option<NodeAvailabilityZoneDiscoveryMethod>,
    last_coverage: Option<NodeAvailabilityZoneCoverage>,
}

impl NodeAvailabilityZoneDiscovery {
    pub(crate) fn preferred_method(&self) -> Option<NodeAvailabilityZoneDiscoveryMethod> {
        self.preferred_method
    }

    pub(crate) fn record_success(&mut self, method: NodeAvailabilityZoneDiscoveryMethod) {
        self.preferred_method = Some(method);
    }

    pub(crate) fn record_failure(&mut self, method: NodeAvailabilityZoneDiscoveryMethod) {
        if self.preferred_method == Some(method) {
            self.preferred_method = None;
        }
    }

    pub(crate) fn should_log_coverage(&mut self, coverage: NodeAvailabilityZoneCoverage) -> bool {
        let previous = self.last_coverage.replace(coverage);
        match (previous, coverage) {
            (None, NodeAvailabilityZoneCoverage::Complete) => false,
            (None, _) => true,
            (Some(previous), current) => previous != current,
        }
    }
}

// Parse slot data from raw redis value.
pub(crate) fn parse_slots(
    raw_slot_resp: Value,
    // The DNS address of the node from which `raw_slot_resp` was received.
    addr_of_answering_node: &str,
    replica_filter: Option<&super::client::ReplicaFilter>,
) -> RedisResult<Vec<SlotRange>> {
    // Parse response.
    let mut slots = Vec::with_capacity(2);
    let mut hosts = HashSet::<ArcStr>::new();

    if let Value::Array(items) = raw_slot_resp {
        let mut iter = items.into_iter();
        while let Some(Value::Array(item)) = iter.next() {
            if item.len() < 3 {
                continue;
            }

            let start = if let Value::Int(start) = item[0] {
                start as u16
            } else {
                continue;
            };

            let end = if let Value::Int(end) = item[1] {
                end as u16
            } else {
                continue;
            };

            let mut try_to_address = |node: Value| {
                let Value::Array(node) = node else {
                    return None;
                };
                if node.len() < 2 {
                    return None;
                }
                // According to the CLUSTER SLOTS documentation:
                // If the received hostname is an empty string or NULL, clients should utilize the hostname of the responding node.
                // However, if the received hostname is "?", it should be regarded as an indication of an unknown node.
                let hostname = if let Value::BulkString(ref ip) = node[0] {
                    let hostname = String::from_utf8_lossy(ip);
                    if hostname.is_empty() || is_wildcard_address(&hostname) {
                        addr_of_answering_node.into()
                    } else if hostname == "?" {
                        return None;
                    } else {
                        hostname
                    }
                } else if let Value::Nil = node[0] {
                    addr_of_answering_node.into()
                } else {
                    return None;
                };
                if hostname.is_empty() {
                    return None;
                }

                let port = if let Value::Int(port) = node[1] {
                    port as u16
                } else {
                    return None;
                };
                // if the hostname was already seen, we'll prefer to take it, in order to reduce fragmentation
                let hostname = match hosts.get(hostname.as_ref()) {
                    Some(host) => host.clone(),
                    None => {
                        let hostname: ArcStr = hostname.into();
                        hosts.insert(hostname.clone());
                        hostname
                    }
                };
                Some(NodeAddress::new(hostname, port))
            };

            let mut iterator = item.into_iter().skip(2);
            let mut primary = None;
            while primary.is_none() {
                let Some(node) = iterator.next() else {
                    break;
                };
                primary = try_to_address(node);
            }
            let Some(primary) = primary else {
                continue;
            };
            let replicas: Vec<NodeAddress> = iterator
                .filter_map(try_to_address)
                .filter(|addr| replica_filter.is_none_or(|f| f(addr)))
                .collect();

            slots.push(SlotRange::new(start, end, primary, replicas));
        }
    }

    Ok(slots)
}

pub(crate) fn parse_cluster_shards_availability_zones(
    raw_shards_resp: &Value,
    addr_of_answering_node: &str,
) -> HashMap<NodeAddress, ArcStr> {
    let mut zones = HashMap::new();
    let Value::Array(shards) = raw_shards_resp else {
        return zones;
    };

    for shard in shards {
        let Some(nodes) = map_get(shard, &["nodes"]) else {
            continue;
        };
        let Value::Array(nodes) = nodes else {
            continue;
        };

        for node in nodes {
            let Some(zone) = cluster_shards_node_zone(node) else {
                continue;
            };
            let Some(addr) = cluster_shards_node_address(node, addr_of_answering_node) else {
                continue;
            };
            zones.insert(addr, zone.into());
        }
    }

    zones
}

pub(crate) fn parse_info_server_availability_zone(info: &str) -> Option<ArcStr> {
    info.lines().find_map(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let (key, value) = line.split_once(':')?;
        let key = normalize_metadata_key(key);
        if key == "availabilityzone" || key.ends_with("availabilityzone") || key == "az" {
            let value = value.trim();
            if value.is_empty() {
                None
            } else {
                Some(value.into())
            }
        } else {
            None
        }
    })
}

pub(crate) fn parse_hostname_availability_zone(host: &str) -> Option<ArcStr> {
    host.split('.')
        .flat_map(candidate_zones_from_label)
        .find(|candidate| {
            is_aws_availability_zone(candidate)
                || is_aws_availability_zone_id(candidate)
                || is_gcp_availability_zone(candidate)
        })
        .map(ArcStr::from)
}

fn cluster_shards_node_zone(node: &Value) -> Option<String> {
    map_get(
        node,
        &[
            "availability-zone",
            "availability_zone",
            "availability zone",
            "availabilityZone",
            "az",
            "zone",
        ],
    )
    .and_then(value_to_string)
    .map(|zone| zone.trim().to_owned())
    .filter(|zone| !zone.is_empty())
}

fn cluster_shards_node_address(node: &Value, addr_of_answering_node: &str) -> Option<NodeAddress> {
    let port = ["port", "tls-port", "tls_port"]
        .into_iter()
        .find_map(|key| match map_get(node, &[key]) {
            Some(Value::Int(port)) => u16::try_from(*port).ok(),
            _ => None,
        })?;

    for key in ["endpoint", "hostname", "host", "ip", "addr"] {
        let Some(host) = map_get(node, &[key]).and_then(value_to_string) else {
            continue;
        };
        let host = host.trim();
        if host == "?" {
            continue;
        }
        if host.is_empty() || is_wildcard_address(host) {
            return Some(NodeAddress::new(addr_of_answering_node, port));
        }
        return Some(NodeAddress::new(host, port));
    }

    None
}

fn map_get<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    let normalized_keys = keys
        .iter()
        .map(|key| normalize_metadata_key(key))
        .collect::<Vec<_>>();

    match value {
        Value::Map(items) => items.iter().find_map(|(key, value)| {
            value_to_string(key).and_then(|key| {
                normalized_keys
                    .contains(&normalize_metadata_key(&key))
                    .then_some(value)
            })
        }),
        Value::Array(items) => items.chunks_exact(2).find_map(|chunk| {
            value_to_string(&chunk[0]).and_then(|key| {
                normalized_keys
                    .contains(&normalize_metadata_key(&key))
                    .then_some(&chunk[1])
            })
        }),
        _ => None,
    }
}

fn value_to_string(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        Value::SimpleString(s) | Value::VerbatimString { text: s, .. } => Some(s.to_owned()),
        Value::Okay => Some("OK".to_owned()),
        _ => None,
    }
}

fn normalize_metadata_key(key: &str) -> String {
    key.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

fn candidate_zones_from_label(label: &str) -> Vec<String> {
    let parts = label
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    let mut candidates = Vec::new();
    for start in 0..parts.len() {
        for len in 2..=4 {
            if start + len <= parts.len() {
                candidates.push(parts[start..start + len].join("-"));
            }
        }
    }
    candidates
}

fn is_aws_availability_zone(candidate: &str) -> bool {
    let parts = candidate.split('-').collect::<Vec<_>>();
    if !(3..=4).contains(&parts.len()) {
        return false;
    }
    let Some(last) = parts.last() else {
        return false;
    };
    if !digits_then_single_letter(last) {
        return false;
    }
    parts[..parts.len() - 1]
        .iter()
        .all(|part| part.chars().all(|c| c.is_ascii_lowercase()))
}

fn is_aws_availability_zone_id(candidate: &str) -> bool {
    let parts = candidate.split('-').collect::<Vec<_>>();
    if parts.len() != 2 {
        return false;
    }
    let region = parts[0];
    let zone_id = parts[1];
    !region.is_empty()
        && region
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit())
        && zone_id
            .strip_prefix("az")
            .is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
}

fn is_gcp_availability_zone(candidate: &str) -> bool {
    let parts = candidate.split('-').collect::<Vec<_>>();
    if parts.len() != 3 {
        return false;
    }
    parts[0].chars().all(|c| c.is_ascii_lowercase())
        && ends_with_digit(parts[1])
        && parts[2].len() == 1
        && parts[2].chars().all(|c| c.is_ascii_lowercase())
}

fn digits_then_single_letter(value: &str) -> bool {
    let mut chars = value.chars().peekable();
    let mut saw_digit = false;
    while matches!(chars.peek(), Some(c) if c.is_ascii_digit()) {
        saw_digit = true;
        chars.next();
    }
    saw_digit
        && chars
            .next()
            .is_some_and(|c| c.is_ascii_lowercase() && chars.next().is_none())
}

fn ends_with_digit(value: &str) -> bool {
    value
        .chars()
        .next_back()
        .is_some_and(|c| c.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn slot_value_with_replicas(start: u16, end: u16, nodes: Vec<(&str, u16)>) -> Value {
        let mut node_values: Vec<Value> = nodes
            .iter()
            .map(|(host, port)| {
                Value::Array(vec![
                    Value::BulkString(host.as_bytes().to_vec()),
                    Value::Int(*port as i64),
                ])
            })
            .collect();
        let mut slot_vec = vec![Value::Int(start as i64), Value::Int(end as i64)];
        slot_vec.append(&mut node_values);
        Value::Array(slot_vec)
    }

    fn slot_value(start: u16, end: u16, node: &str, port: u16) -> Value {
        slot_value_with_replicas(start, end, vec![(node, port)])
    }

    fn bulk(value: &str) -> Value {
        Value::BulkString(value.as_bytes().to_vec())
    }

    #[test]
    fn parse_slots_returns_slots_with_host_name_if_missing() {
        let view = Value::Array(vec![slot_value(0, 4000, "", 6379)]);

        let slots = parse_slots(view, "node", None).unwrap();
        assert_eq!(slots[0].master, "node:6379");
    }

    #[test]
    fn parse_slots_treats_wildcard_hostnames_as_answering_node() {
        // Master advertised as 0.0.0.0 should be treated as the answering node's host
        let view = Value::Array(vec![slot_value_with_replicas(
            0,
            100,
            vec![("0.0.0.0", 7000)],
        )]);
        let slots = parse_slots(view, "answer.host", None).unwrap();
        assert_eq!(slots[0].master, "answer.host:7000");

        // IPv6 wildcard :: similarly falls back to answering node
        let view_v6 = Value::Array(vec![slot_value_with_replicas(200, 300, vec![("::", 7001)])]);
        let slots_v6 = parse_slots(view_v6, "answer6.host", None).unwrap();
        assert_eq!(slots_v6[0].master, "answer6.host:7001");
    }

    #[test]
    fn parse_slots_applies_replica_filter() {
        // Shard: primary "p:6379" + replicas "r1:6379", "r2:6379", "r3:6379"
        let view = Value::Array(vec![slot_value_with_replicas(
            0,
            100,
            vec![("p", 6379), ("r1", 6379), ("r2", 6379), ("r3", 6379)],
        )]);

        // Keep only "r2" — primary must pass through untouched.
        let keep_r2 = |addr: &NodeAddress| addr.host() == "r2";
        let slots = parse_slots(view, "answer", Some(&keep_r2)).unwrap();

        assert_eq!(slots[0].master, "p:6379");
        assert_eq!(slots[0].replicas.len(), 1);
        assert_eq!(slots[0].replicas[0], "r2:6379");
    }

    #[test]
    fn parse_slots_filter_dropping_all_replicas_leaves_primary() {
        let view = Value::Array(vec![slot_value_with_replicas(
            0,
            100,
            vec![("p", 6379), ("r1", 6379), ("r2", 6379)],
        )]);

        let drop_all = |_addr: &NodeAddress| false;
        let slots = parse_slots(view, "answer", Some(&drop_all)).unwrap();

        assert_eq!(slots[0].master, "p:6379");
        assert!(slots[0].replicas.is_empty());
    }

    #[test]
    fn parse_cluster_shards_availability_zones_from_resp3_maps() {
        let view = Value::Array(vec![Value::Map(vec![(
            bulk("nodes"),
            Value::Array(vec![
                Value::Map(vec![
                    (bulk("endpoint"), bulk("primary.example.com")),
                    (bulk("port"), Value::Int(6379)),
                    (bulk("availability-zone"), bulk("us-east-1a")),
                ]),
                Value::Map(vec![
                    (bulk("hostname"), bulk("replica.example.com")),
                    (bulk("port"), Value::Int(6380)),
                    (bulk("availability_zone"), bulk("us-east-1b")),
                ]),
            ]),
        )])]);

        let zones = parse_cluster_shards_availability_zones(&view, "answer.example.com");

        assert_eq!(
            zones.get(&NodeAddress::new("primary.example.com", 6379)),
            Some(&ArcStr::from("us-east-1a"))
        );
        assert_eq!(
            zones.get(&NodeAddress::new("replica.example.com", 6380)),
            Some(&ArcStr::from("us-east-1b"))
        );
    }

    #[test]
    fn parse_cluster_shards_availability_zones_from_flat_arrays() {
        let view = Value::Array(vec![Value::Array(vec![
            bulk("nodes"),
            Value::Array(vec![Value::Array(vec![
                bulk("endpoint"),
                bulk(""),
                bulk("port"),
                Value::Int(6379),
                bulk("az"),
                bulk("use1-az1"),
            ])]),
        ])]);

        let zones = parse_cluster_shards_availability_zones(&view, "answer.example.com");

        assert_eq!(
            zones.get(&NodeAddress::new("answer.example.com", 6379)),
            Some(&ArcStr::from("use1-az1"))
        );
    }

    #[test]
    fn parse_info_server_availability_zone_handles_elasticache_key() {
        let info = "# Server\r\nredis_version:7.2.4\r\navailability_zone:us-east-1b\r\n";

        assert_eq!(
            parse_info_server_availability_zone(info),
            Some(ArcStr::from("us-east-1b"))
        );
    }

    #[test]
    fn parse_hostname_availability_zone_supports_common_cloud_tokens() {
        assert_eq!(
            parse_hostname_availability_zone("cache-0001.us-east-1b.cache.amazonaws.com"),
            Some(ArcStr::from("us-east-1b"))
        );
        assert_eq!(
            parse_hostname_availability_zone("node-use1-az2.example.com"),
            Some(ArcStr::from("use1-az2"))
        );
        assert_eq!(
            parse_hostname_availability_zone("redis.us-central1-a.example.internal"),
            Some(ArcStr::from("us-central1-a"))
        );
    }

    #[test]
    fn availability_zone_discovery_logs_only_important_coverage_transitions() {
        let mut discovery = NodeAvailabilityZoneDiscovery::default();

        assert!(!discovery.should_log_coverage(NodeAvailabilityZoneCoverage::Complete));
        assert!(!discovery.should_log_coverage(NodeAvailabilityZoneCoverage::Complete));
        assert!(discovery.should_log_coverage(NodeAvailabilityZoneCoverage::None));
        assert!(!discovery.should_log_coverage(NodeAvailabilityZoneCoverage::None));
        assert!(discovery.should_log_coverage(NodeAvailabilityZoneCoverage::Partial));
        assert!(!discovery.should_log_coverage(NodeAvailabilityZoneCoverage::Partial));
        assert!(discovery.should_log_coverage(NodeAvailabilityZoneCoverage::Complete));
    }
}
