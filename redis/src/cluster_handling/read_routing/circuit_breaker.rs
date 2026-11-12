//! A per-node circuit breaker for replica reads.
//!
//! Per-request read fallback only protects the request that hit the failure;
//! every new request still starts at the strategy's preferred node. When that
//! node is resyncing (`LOADING`), cut off from its primary (`MASTERDOWN`), or
//! stalled (timeouts), each request pays for the failure before falling back.
//! The breaker remembers the failure across requests and across every cluster
//! connection that shares it, so strategies can route around the node until
//! it answers again.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use crate::cluster_handling::NodeAddress;
use crate::errors::{ErrorKind, RedisError, ServerErrorKind};

/// How a read sent to a node turned out, as far as that node's health goes.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ReadOutcome {
    /// The node answered. Command-level errors such as `WRONGTYPE` or a
    /// redirect still count: the node is serving.
    Responded,
    /// The node did not answer: a timeout, a dropped or refused connection,
    /// or a protocol failure. Repeated failures open the breaker.
    Failed,
    /// The node answered that it cannot serve reads (`LOADING` or
    /// `MASTERDOWN`). This opens the breaker immediately.
    Unavailable,
}

impl ReadOutcome {
    /// Classifies a request error, or returns `None` when the error says
    /// nothing about the node (for example a client-side type error).
    pub(crate) fn from_error(err: &RedisError) -> Option<Self> {
        match err.kind() {
            ErrorKind::Server(ServerErrorKind::BusyLoading | ServerErrorKind::MasterDown) => {
                Some(Self::Unavailable)
            }
            ErrorKind::Server(ServerErrorKind::ClusterDown) => Some(Self::Failed),
            ErrorKind::Server(_) => Some(Self::Responded),
            ErrorKind::Io | ErrorKind::Parse | ErrorKind::ClusterConnectionNotFound => {
                Some(Self::Failed)
            }
            _ => None,
        }
    }

    /// Returns whether `err` means the node itself cannot serve reads right
    /// now, so the read should go to another node instead of waiting.
    pub(crate) fn is_node_unavailable(err: &RedisError) -> bool {
        Self::from_error(err) == Some(Self::Unavailable)
    }
}

/// Tuning for [`ReadCircuitBreaker`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadCircuitBreakerConfig {
    /// Consecutive [`Failed`](ReadOutcome::Failed) reads that open the
    /// breaker. `LOADING` and `MASTERDOWN` open it on the first reply.
    pub failure_threshold: u32,
    /// How long a node stays open before one probe read is let through.
    pub open_duration: Duration,
    /// Upper bound for the open duration, which doubles after each failed
    /// probe.
    pub max_open_duration: Duration,
}

impl Default for ReadCircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 3,
            open_duration: Duration::from_secs(2),
            max_open_duration: Duration::from_secs(30),
        }
    }
}

#[derive(Debug)]
enum NodeState {
    Closed {
        consecutive_failures: u32,
    },
    Open {
        /// Until this instant no reads are routed to the node. Once it
        /// passes, one probe read is let through and the deadline moves
        /// forward by `open_duration`, so a probe whose outcome is never
        /// reported cannot wedge the node open or let a flood through.
        until: Instant,
        open_duration: Duration,
        /// A probe was granted since the node opened. Only outcomes observed
        /// while probing close or re-open the node; results of reads that
        /// were already in flight when it opened are ignored.
        probing: bool,
    },
}

/// Shared per-node read health.
///
/// Clones share state, so one handle can be given to every cluster
/// connection in a process: once any connection sees a node fail, all of
/// them route around it.
#[derive(Debug, Clone)]
pub struct ReadCircuitBreaker {
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    config: ReadCircuitBreakerConfig,
    /// Number of entries in `nodes`. Healthy clusters have no entries, so
    /// the hot path is a single relaxed load.
    tracked: AtomicUsize,
    nodes: RwLock<HashMap<NodeAddress, Arc<Mutex<NodeState>>>>,
}

impl ReadCircuitBreaker {
    /// Creates a breaker with the given configuration.
    pub fn new(config: ReadCircuitBreakerConfig) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                tracked: AtomicUsize::new(0),
                nodes: RwLock::new(HashMap::new()),
            }),
        }
    }

    /// The breaker's configuration.
    pub fn config(&self) -> &ReadCircuitBreakerConfig {
        &self.inner.config
    }

    /// Returns whether reads may be routed to `node` now.
    ///
    /// A `true` for an open node whose deadline has passed grants the single
    /// probe for this interval, so call this only when about to route a read.
    pub fn is_available(&self, node: &NodeAddress) -> bool {
        self.is_available_at(node, Instant::now())
    }

    /// Returns whether `node` is open, including while it waits on a probe.
    /// Unlike [`is_available`](Self::is_available) this never grants a probe.
    pub fn is_open(&self, node: &NodeAddress) -> bool {
        self.node_state(node).is_some_and(|state| {
            matches!(
                *state.lock().expect("Lock poisoned"),
                NodeState::Open { .. }
            )
        })
    }

    /// Returns the nodes currently open (including those waiting on a probe).
    pub fn open_nodes(&self) -> Vec<NodeAddress> {
        if self.inner.tracked.load(Ordering::Relaxed) == 0 {
            return Vec::new();
        }
        let nodes = self.inner.nodes.read().expect("Lock poisoned");
        let mut open: Vec<_> = nodes
            .iter()
            .filter(|(_, state)| {
                matches!(
                    *state.lock().expect("Lock poisoned"),
                    NodeState::Open { .. }
                )
            })
            .map(|(node, _)| node.clone())
            .collect();
        open.sort();
        open
    }

    /// Records the outcome of a read sent to `node`.
    pub fn record(&self, node: &NodeAddress, outcome: ReadOutcome) {
        self.record_at(node, outcome, Instant::now());
    }

    fn node_state(&self, node: &NodeAddress) -> Option<Arc<Mutex<NodeState>>> {
        if self.inner.tracked.load(Ordering::Relaxed) == 0 {
            return None;
        }
        self.inner
            .nodes
            .read()
            .expect("Lock poisoned")
            .get(node)
            .cloned()
    }

    pub(crate) fn is_available_at(&self, node: &NodeAddress, now: Instant) -> bool {
        let Some(state) = self.node_state(node) else {
            return true;
        };
        let mut state = state.lock().expect("Lock poisoned");
        match &mut *state {
            NodeState::Closed { .. } => true,
            NodeState::Open {
                until,
                open_duration,
                probing,
            } => {
                if now < *until {
                    return false;
                }
                *until = now + *open_duration;
                *probing = true;
                true
            }
        }
    }

    pub(crate) fn record_at(&self, node: &NodeAddress, outcome: ReadOutcome, now: Instant) {
        match outcome {
            ReadOutcome::Responded => self.record_response(node),
            ReadOutcome::Failed | ReadOutcome::Unavailable => {
                self.record_failure(node, outcome, now)
            }
        }
    }

    fn record_response(&self, node: &NodeAddress) {
        let Some(state) = self.node_state(node) else {
            return;
        };
        {
            let state = state.lock().expect("Lock poisoned");
            if let NodeState::Open { probing: false, .. } = *state {
                return;
            }
        }
        let mut nodes = self.inner.nodes.write().expect("Lock poisoned");
        // Re-check under the write lock: another thread may have re-opened
        // the node or already removed it.
        let Some(state) = nodes.get(node) else {
            return;
        };
        let was_open = match *state.lock().expect("Lock poisoned") {
            NodeState::Open { probing: false, .. } => return,
            NodeState::Open { probing: true, .. } => true,
            NodeState::Closed { .. } => false,
        };
        nodes.remove(node);
        self.inner.tracked.store(nodes.len(), Ordering::Relaxed);
        drop(nodes);
        if was_open {
            log::info!("redis read circuit breaker closed for {node}: probe read succeeded");
        }
    }

    fn record_failure(&self, node: &NodeAddress, outcome: ReadOutcome, now: Instant) {
        // Hold the map lock through the state update so a concurrent response
        // cannot remove the entry while this failure is being recorded.
        let mut nodes = self.inner.nodes.write().expect("Lock poisoned");
        let state = nodes
            .entry(node.clone())
            .or_insert_with(|| {
                Arc::new(Mutex::new(NodeState::Closed {
                    consecutive_failures: 0,
                }))
            })
            .clone();
        self.inner.tracked.store(nodes.len(), Ordering::Relaxed);

        let config = &self.inner.config;
        let mut state = state.lock().expect("Lock poisoned");
        match &mut *state {
            NodeState::Closed {
                consecutive_failures,
            } => {
                *consecutive_failures = consecutive_failures.saturating_add(1);
                if outcome == ReadOutcome::Unavailable
                    || *consecutive_failures >= config.failure_threshold.max(1)
                {
                    log::warn!(
                        "redis read circuit breaker opened for {node} for {:?}: {outcome:?} after {consecutive_failures} consecutive failure(s)",
                        config.open_duration
                    );
                    *state = NodeState::Open {
                        until: now + config.open_duration,
                        open_duration: config.open_duration,
                        probing: false,
                    };
                }
            }
            NodeState::Open {
                until,
                open_duration,
                probing,
            } => {
                if !*probing {
                    return;
                }
                *open_duration = (*open_duration * 2).min(config.max_open_duration);
                *until = now + *open_duration;
                *probing = false;
                log::warn!(
                    "redis read circuit breaker re-opened for {node} for {open_duration:?}: probe read {outcome:?}"
                );
            }
        }
    }
}

impl Default for ReadCircuitBreaker {
    fn default() -> Self {
        Self::new(ReadCircuitBreakerConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(port: u16) -> NodeAddress {
        NodeAddress::from_parts("replica".into(), port)
    }

    fn breaker() -> ReadCircuitBreaker {
        ReadCircuitBreaker::new(ReadCircuitBreakerConfig {
            failure_threshold: 3,
            open_duration: Duration::from_secs(2),
            max_open_duration: Duration::from_secs(5),
        })
    }

    #[test]
    fn unavailable_opens_immediately_and_probe_closes() {
        let breaker = breaker();
        let t0 = Instant::now();
        let n = node(1);

        breaker.record_at(&n, ReadOutcome::Unavailable, t0);
        assert!(!breaker.is_available_at(&n, t0));
        assert_eq!(breaker.open_nodes(), vec![n.clone()]);

        // An in-flight read that completes while open does not close it.
        breaker.record_at(&n, ReadOutcome::Responded, t0);
        assert!(!breaker.is_available_at(&n, t0 + Duration::from_secs(1)));

        // One probe per interval once the deadline passes.
        let t1 = t0 + Duration::from_secs(2);
        assert!(breaker.is_available_at(&n, t1));
        assert!(!breaker.is_available_at(&n, t1));

        breaker.record_at(&n, ReadOutcome::Responded, t1);
        assert!(breaker.is_available_at(&n, t1));
        assert!(breaker.open_nodes().is_empty());
        assert_eq!(breaker.inner.tracked.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn failures_open_after_threshold_and_success_resets_count() {
        let breaker = breaker();
        let t0 = Instant::now();
        let n = node(2);

        breaker.record_at(&n, ReadOutcome::Failed, t0);
        breaker.record_at(&n, ReadOutcome::Failed, t0);
        breaker.record_at(&n, ReadOutcome::Responded, t0);
        breaker.record_at(&n, ReadOutcome::Failed, t0);
        breaker.record_at(&n, ReadOutcome::Failed, t0);
        assert!(breaker.is_available_at(&n, t0));

        breaker.record_at(&n, ReadOutcome::Failed, t0);
        assert!(!breaker.is_available_at(&n, t0));
    }

    #[test]
    fn failed_probe_backs_off_up_to_max() {
        let breaker = breaker();
        let t0 = Instant::now();
        let n = node(3);

        breaker.record_at(&n, ReadOutcome::Unavailable, t0);

        let t1 = t0 + Duration::from_secs(2);
        assert!(breaker.is_available_at(&n, t1));
        breaker.record_at(&n, ReadOutcome::Unavailable, t1);
        // Doubled to 4s.
        assert!(!breaker.is_available_at(&n, t1 + Duration::from_secs(3)));
        let t2 = t1 + Duration::from_secs(4);
        assert!(breaker.is_available_at(&n, t2));
        breaker.record_at(&n, ReadOutcome::Failed, t2);
        // Capped at 5s rather than 8s.
        assert!(!breaker.is_available_at(&n, t2 + Duration::from_millis(4999)));
        assert!(breaker.is_available_at(&n, t2 + Duration::from_secs(5)));
    }

    #[test]
    fn unreported_probe_does_not_wedge_node() {
        let breaker = breaker();
        let t0 = Instant::now();
        let n = node(4);

        breaker.record_at(&n, ReadOutcome::Unavailable, t0);
        let t1 = t0 + Duration::from_secs(2);
        assert!(breaker.is_available_at(&n, t1));
        // No outcome reported; another probe is granted next interval.
        assert!(!breaker.is_available_at(&n, t1 + Duration::from_secs(1)));
        assert!(breaker.is_available_at(&n, t1 + Duration::from_secs(2)));
    }

    #[test]
    fn clones_share_state() {
        let breaker = breaker();
        let clone = breaker.clone();
        let n = node(5);
        breaker.record(&n, ReadOutcome::Unavailable);
        assert!(!clone.is_available(&n));
    }

    fn assert_unavailable_survives_concurrent_success(pretrack_nodes: bool) {
        let breaker = breaker();
        let now = Instant::now();
        let nodes: Vec<_> = (0..100_000)
            .map(|i| NodeAddress::new(format!("replica-{i}"), 6379))
            .collect();
        if pretrack_nodes {
            for node in &nodes {
                breaker.record_at(node, ReadOutcome::Failed, now);
            }
        }

        let barrier = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for node in &nodes {
                    barrier.wait();
                    breaker.record_at(node, ReadOutcome::Unavailable, now);
                }
            });
            for node in &nodes {
                barrier.wait();
                breaker.record_at(node, ReadOutcome::Responded, now);
            }
        });

        // Either serial order leaves the breaker open: a success before
        // LOADING is followed by an open, and a success after it is ignored.
        for node in &nodes {
            assert!(breaker.is_open(node), "lost unavailable outcome for {node}");
        }
        assert_eq!(breaker.inner.tracked.load(Ordering::Relaxed), nodes.len());
    }

    #[test]
    fn concurrent_success_cannot_discard_tracked_unavailable() {
        assert_unavailable_survives_concurrent_success(true);
    }

    #[test]
    fn concurrent_success_cannot_discard_first_unavailable() {
        assert_unavailable_survives_concurrent_success(false);
    }

    #[test]
    fn classifies_errors() {
        let loading: RedisError = (ErrorKind::Server(ServerErrorKind::BusyLoading), "x").into();
        let masterdown: RedisError = (ErrorKind::Server(ServerErrorKind::MasterDown), "x").into();
        let moved: RedisError = (ErrorKind::Server(ServerErrorKind::Moved), "x").into();
        let io: RedisError = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out").into();
        let client: RedisError = (ErrorKind::Client, "x").into();

        assert_eq!(
            ReadOutcome::from_error(&loading),
            Some(ReadOutcome::Unavailable)
        );
        assert_eq!(
            ReadOutcome::from_error(&masterdown),
            Some(ReadOutcome::Unavailable)
        );
        assert_eq!(
            ReadOutcome::from_error(&moved),
            Some(ReadOutcome::Responded)
        );
        assert_eq!(ReadOutcome::from_error(&io), Some(ReadOutcome::Failed));
        assert_eq!(ReadOutcome::from_error(&client), None);
        assert!(ReadOutcome::is_node_unavailable(&loading));
        assert!(!ReadOutcome::is_node_unavailable(&io));
    }
}
