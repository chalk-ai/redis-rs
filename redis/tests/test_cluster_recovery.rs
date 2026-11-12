#![cfg(feature = "cluster")]
mod support;

use redis::{
    RedisResult, Value,
    cluster::{ClusterClient, cluster_pipe},
    cmd, parse_redis_value,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use support::*;

#[test]
fn failed_pipeline_redirects_follow_refreshed_topology() {
    for (name, redirect, timeout) in [
        ("compact_moved_clusterdown", "MOVED", false),
        ("compact_ask_clusterdown", "ASK", false),
        ("compact_ask_connect_timeout", "ASK", true),
    ] {
        let changed = Arc::new(AtomicBool::new(false));
        if timeout {
            set_connect_timeouts(name, [6381]);
        }
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder([format!("redis://{name}")])
                .periodic_topology_refresh(Duration::ZERO)
                .min_slot_refresh_interval(Duration::from_secs(3600))
                .min_retry_wait(1)
                .max_retry_wait(1)
                .retries(3),
            name,
            {
                let changed = changed.clone();
                move |command, port| {
                    if is_connection_check(command) || contains_slice(command, b"ASKING") {
                        return Err(Ok(Value::Okay));
                    }
                    if contains_slice(command, b"SLOTS") {
                        if port == 6381 {
                            return Err(Ok(Value::Array(vec![])));
                        }
                        return respond_startup_with_replica_using_config(
                            name,
                            command,
                            Some(vec![MockSlotRange {
                                primary_port: if changed.load(Ordering::SeqCst) {
                                    6382
                                } else {
                                    6379
                                },
                                replica_ports: vec![],
                                slot_range: 0..16383,
                            }]),
                        );
                    }
                    match port {
                        6379 => {
                            if timeout {
                                changed.store(true, Ordering::SeqCst);
                            }
                            Err(parse_redis_value(
                                format!("-{redirect} 6918 {name}:6381\r\n").as_bytes(),
                            ))
                        }
                        6381 => {
                            changed.store(true, Ordering::SeqCst);
                            Err(parse_redis_value(b"-CLUSTERDOWN slot not served\r\n"))
                        }
                        6382 => Err(Ok(Value::Int(123))),
                        _ => unreachable!(),
                    }
                }
            },
        );
        std::thread::sleep(Duration::from_millis(1600));
        assert_eq!(
            cluster_pipe().get("test").query::<(i64,)>(&mut connection),
            Ok((123,))
        );
        assert_eq!(
            cmd("GET").arg("test").query::<i64>(&mut connection),
            Ok(123)
        );
    }
}

#[test]
fn connection_timeout_refreshes_single_request() {
    let name = "compact_single_connect_timeout";
    let changed = Arc::new(AtomicBool::new(false));
    set_connect_timeouts(name, [6379]);
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder([format!("redis://{name}:6382")])
            .periodic_topology_refresh(Duration::ZERO)
            .retries(1),
        name,
        {
            let changed = changed.clone();
            move |command, port| {
                if is_connection_check(command) {
                    return Err(Ok(Value::Okay));
                }
                if contains_slice(command, b"SLOTS") {
                    return respond_startup_with_replica_using_config(
                        name,
                        command,
                        Some(vec![
                            MockSlotRange {
                                primary_port: if changed.load(Ordering::SeqCst) {
                                    6381
                                } else {
                                    6379
                                },
                                replica_ports: vec![],
                                slot_range: 0..8191,
                            },
                            MockSlotRange {
                                primary_port: 6382,
                                replica_ports: vec![],
                                slot_range: 8192..16383,
                            },
                        ]),
                    );
                }
                assert_eq!(port, 6381, "write did not reach the new owner");
                Err(Ok(Value::Okay))
            }
        },
    );
    std::thread::sleep(Duration::from_millis(1600));
    changed.store(true, Ordering::SeqCst);
    cmd("SET")
        .arg("test")
        .arg("value")
        .exec(&mut connection)
        .unwrap();
}

#[test]
fn response_timeouts_do_not_refresh_or_reconnect() {
    for (name, asking) in [
        ("compact_response_timeout", false),
        ("compact_asking_timeout", true),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let fetches = Arc::new(AtomicUsize::new(0));
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder([format!("redis://{name}")])
                .periodic_topology_refresh(Duration::ZERO)
                .retries(3),
            name,
            {
                let fetches = fetches.clone();
                move |command, port| {
                    if contains_slice(command, b"SLOTS") {
                        fetches.fetch_add(1, Ordering::SeqCst);
                    }
                    respond_startup(name, command)?;
                    if asking && port == 6379 {
                        return Err(parse_redis_value(
                            format!("-ASK 6918 {name}:6381\r\n").as_bytes(),
                        ));
                    }
                    let preamble = contains_slice(command, b"ASKING");
                    if (!asking || preamble) && calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        return Err(Err(
                            std::io::Error::from(std::io::ErrorKind::TimedOut).into()
                        ));
                    }
                    Err(Ok(if preamble {
                        Value::Okay
                    } else {
                        Value::Int(123)
                    }))
                }
            },
        );
        std::thread::sleep(Duration::from_millis(1600));
        let before = fetches.load(Ordering::SeqCst);
        let _ = take_connects(name);
        assert_eq!(
            cmd("GET").arg("test").query::<i64>(&mut connection),
            Ok(123)
        );
        assert_eq!(fetches.load(Ordering::SeqCst), before);
        assert_eq!(
            take_connects(name),
            if asking { vec![6381] } else { vec![] }
        );
    }
}

#[derive(Clone, Copy)]
enum MixedFailure {
    Reconnect,
    Moved,
}
struct MixedCase {
    failure: MixedFailure,
    rounds: usize,
    expire_error_floor: bool,
    moved_interval: Duration,
}
impl Default for MixedCase {
    fn default() -> Self {
        Self {
            failure: MixedFailure::Reconnect,
            rounds: 1,
            expire_error_floor: true,
            moved_interval: Duration::from_secs(3600),
        }
    }
}

fn mixed_pipeline(name: &'static str, case: MixedCase) -> (RedisResult<Vec<Value>>, Vec<usize>) {
    const OWNERS: [(u16, u16); 3] = [(6379, 6381), (6383, 6384), (6385, 6386)];
    let stage = Arc::new(AtomicUsize::new(0));
    let fetches: Arc<[AtomicUsize; 3]> = Arc::new(std::array::from_fn(|_| AtomicUsize::new(0)));
    let healthy_reads = Arc::new(AtomicUsize::new(0));
    set_connect_timeouts(name, OWNERS.iter().take(case.rounds).map(|(a, _)| *a));
    let MockEnv {
        mut connection,
        handler: _handler,
        ..
    } = MockEnv::with_client_builder(
        ClusterClient::builder([format!("redis://{name}:6382")])
            .periodic_topology_refresh(Duration::ZERO)
            .min_slot_refresh_interval(case.moved_interval)
            .retries(case.rounds as u32),
        name,
        {
            let stage = stage.clone();
            let fetches = fetches.clone();
            let healthy_reads = healthy_reads.clone();
            move |command, port| {
                if is_connection_check(command) {
                    return Err(Ok(Value::Okay));
                }
                if contains_slice(command, b"SLOTS") {
                    let stage = stage.load(Ordering::SeqCst);
                    if stage > 0 {
                        assert_eq!(
                            healthy_reads.load(Ordering::SeqCst),
                            1,
                            "refresh before drain"
                        );
                    }
                    fetches[stage].fetch_add(1, Ordering::SeqCst);
                    return respond_startup_with_replica_using_config(
                        name,
                        command,
                        Some(vec![
                            MockSlotRange {
                                primary_port: OWNERS[stage].0,
                                replica_ports: vec![],
                                slot_range: 0..8191,
                            },
                            MockSlotRange {
                                primary_port: OWNERS[stage].1,
                                replica_ports: vec![],
                                slot_range: 8192..14000,
                            },
                            MockSlotRange {
                                primary_port: 6382,
                                replica_ports: vec![],
                                slot_range: 14001..16383,
                            },
                        ]),
                    );
                }
                if port == 6382 {
                    healthy_reads.fetch_add(1, Ordering::SeqCst);
                }
                if let Some(round) = OWNERS
                    .iter()
                    .take(case.rounds)
                    .position(|(_, b)| *b == port)
                {
                    stage.store(round + 1, Ordering::SeqCst);
                    return match case.failure {
                        MixedFailure::Reconnect => {
                            set_connect_errors(
                                name,
                                OWNERS.iter().take(round + 1).map(|(_, b)| *b),
                            );
                            Err(Err(std::io::Error::from(
                                std::io::ErrorKind::ConnectionReset,
                            )
                            .into()))
                        }
                        MixedFailure::Moved => Err(parse_redis_value(
                            format!("-MOVED 12182 {name}:{}\r\n", OWNERS[round + 1].1).as_bytes(),
                        )),
                    };
                }
                Err(Ok(Value::Int(port.into())))
            }
        },
    );
    if case.expire_error_floor {
        std::thread::sleep(Duration::from_millis(1600));
    }
    let initial_fetches = fetches[0].load(Ordering::SeqCst);
    let result = cluster_pipe()
        .get("test")
        .get("{foo}test")
        .get("stable")
        .query(&mut connection);
    assert_eq!(
        fetches[0].load(Ordering::SeqCst),
        initial_fetches,
        "refresh before failure replies"
    );
    assert_eq!(
        healthy_reads.load(Ordering::SeqCst),
        1,
        "successful command was replayed"
    );
    let counts = fetches
        .iter()
        .skip(1)
        .take(case.rounds)
        .map(|count| count.load(Ordering::SeqCst))
        .collect::<Vec<_>>();
    assert!(
        counts.iter().all(|count| *count <= 1),
        "duplicate refresh in one round: {counts:?}"
    );
    (result, counts)
}

#[test]
fn mixed_failures_refresh_once_per_round_after_draining_all_replies() {
    let (result, counts) = mixed_pipeline(
        "compact_mixed_reconnect",
        MixedCase {
            rounds: 2,
            ..MixedCase::default()
        },
    );
    assert_eq!(
        result,
        Ok(vec![Value::Int(6385), Value::Int(6386), Value::Int(6382)])
    );
    assert_eq!(counts, vec![1, 1]);
}

#[test]
fn error_and_moved_refresh_cooldowns_are_combined() {
    for (name, expired, interval, expected) in [
        (
            "compact_mixed_error_due",
            true,
            Duration::from_secs(3600),
            1,
        ),
        ("compact_mixed_moved_due", false, Duration::ZERO, 1),
        (
            "compact_mixed_neither_due",
            false,
            Duration::from_secs(3600),
            0,
        ),
    ] {
        let (result, counts) = mixed_pipeline(
            name,
            MixedCase {
                failure: MixedFailure::Moved,
                expire_error_floor: expired,
                moved_interval: interval,
                ..MixedCase::default()
            },
        );
        assert_eq!(counts, vec![expected]);
        if expected == 0 {
            assert!(result.unwrap_err().is_timeout());
        } else {
            assert_eq!(
                result,
                Ok(vec![Value::Int(6383), Value::Int(6384), Value::Int(6382)])
            );
        }
    }
}
