#![cfg(feature = "cluster")]
mod support;

use redis::{
    Value,
    cluster::{ClusterClient, cluster_pipe},
    cmd, parse_redis_value,
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use support::*;

#[test]
fn failed_pipeline_redirects_follow_refreshed_topology() {
    for (name, redirect) in [
        ("compact_moved_clusterdown", "MOVED"),
        ("compact_ask_clusterdown", "ASK"),
    ] {
        let changed = Arc::new(AtomicBool::new(false));
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
                        6379 => Err(parse_redis_value(
                            format!("-{redirect} 6918 {name}:6381\r\n").as_bytes(),
                        )),
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
