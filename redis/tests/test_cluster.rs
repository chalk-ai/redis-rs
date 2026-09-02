#![cfg(feature = "cluster")]
mod support;

#[cfg(test)]
mod cluster {
    use std::sync::{
        Arc,
        atomic::{self, AtomicI32, Ordering},
    };

    use crate::support::*;
    use assert_matches::assert_matches;
    use redis::{
        Commands, ConnectionLike, ErrorKind, RedisError, ServerErrorKind, Value,
        cluster::{ClusterClient, ClusterConnection, cluster_pipe},
        cluster_read_routing::{
            RandomReplicaStrategy, RoundRobinReplicaStrategy, UniformRandom,
            ZonalReadRoutingStrategy,
        },
        cluster_routing::{
            MultipleNodeRoutingInfo, Route, RoutingInfo, SingleNodeRoutingInfo, SlotAddr,
        },
        cmd, parse_redis_value,
    };
    use redis_test::{
        cluster::{RedisCluster, RedisClusterConfiguration},
        redis_value,
        server::use_protocol,
    };

    fn smoke_test_connection(mut con: ClusterConnection) {
        redis::cmd("SET")
            .arg("{x}key1")
            .arg(b"foo")
            .exec(&mut con)
            .unwrap();
        redis::cmd("SET")
            .arg(&["{x}key2", "bar"])
            .exec(&mut con)
            .unwrap();

        assert_eq!(
            redis::cmd("MGET")
                .arg(&["{x}key1", "{x}key2"])
                .query(&mut con),
            Ok(("foo".to_string(), b"bar".to_vec()))
        );
    }

    #[test]
    fn test_cluster_basics() {
        let cluster = TestClusterContext::new();
        smoke_test_connection(cluster.connection());
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn test_default_reject_invalid_hostnames() {
        use redis_test::cluster::ClusterType;

        if ClusterType::get_intended() != ClusterType::TcpTls {
            // Only TLS causes invalid certificates to be rejected as desired.
            return;
        }

        let cluster = TestClusterContext::new_with_config(RedisClusterConfiguration {
            tls_insecure: false,
            certs_with_ip_alts: false,
            ..Default::default()
        });
        assert!(cluster.client.get_connection().is_err());
    }

    #[cfg(feature = "tls-rustls-insecure")]
    #[test]
    fn test_danger_accept_invalid_hostnames() {
        use redis_test::cluster::ClusterType;

        if ClusterType::get_intended() != ClusterType::TcpTls {
            // No point testing this TLS-specific mode in non-TLS configurations.
            return;
        }

        let cluster = TestClusterContext::new_with_config_and_builder(
            RedisClusterConfiguration {
                tls_insecure: false,
                certs_with_ip_alts: false,
                ..Default::default()
            },
            |builder| builder.danger_accept_invalid_hostnames(true),
        );

        smoke_test_connection(cluster.connection());
    }

    #[test]
    fn test_cluster_with_username_and_password() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(|builder| {
            builder
                .username(RedisCluster::username())
                .password(RedisCluster::password())
        });
        cluster.disable_default_user();

        smoke_test_connection(cluster.connection());
    }

    #[test]
    fn test_cluster_with_bad_password() {
        let cluster = TestClusterContext::new_with_cluster_client_builder(|builder| {
            builder
                .username(RedisCluster::username())
                .password("not the right password")
        });
        assert!(cluster.client.get_connection().is_err());
    }

    #[test]
    fn test_cluster_read_from_replicas() {
        let cluster = TestClusterContext::new_with_config_and_builder(
            RedisClusterConfiguration::single_replica_config(),
            |builder| builder.read_routing_strategy(RandomReplicaStrategy),
        );
        let mut con = cluster.connection();

        // Write commands would go to the primary nodes
        redis::cmd("SET")
            .arg("{x}key1")
            .arg(b"foo")
            .exec(&mut con)
            .unwrap();
        redis::cmd("SET")
            .arg(&["{x}key2", "bar"])
            .exec(&mut con)
            .unwrap();

        // Read commands would go to the replica nodes
        assert_eq!(
            redis::cmd("MGET")
                .arg(&["{x}key1", "{x}key2"])
                .query(&mut con),
            Ok(("foo".to_string(), b"bar".to_vec()))
        );
    }

    #[test]
    fn test_cluster_eval() {
        let cluster = TestClusterContext::new();
        let mut con = cluster.connection();

        let rv = redis::cmd("EVAL")
            .arg(
                r#"
            redis.call("SET", KEYS[1], "1");
            redis.call("SET", KEYS[2], "2");
            return redis.call("MGET", KEYS[1], KEYS[2]);
        "#,
            )
            .arg("2")
            .arg("{x}a")
            .arg("{x}b")
            .query(&mut con);

        assert_eq!(rv, Ok(("1".to_string(), "2".to_string())));
    }

    #[test]
    fn test_cluster_resp3() {
        if !use_protocol().supports_resp3() {
            return;
        }
        let cluster = TestClusterContext::new();

        let mut connection = cluster.connection();

        let _: () = connection.hset("hash", "foo", "baz").unwrap();
        let _: () = connection.hset("hash", "bar", "foobar").unwrap();
        let result: Value = connection.hgetall("hash").unwrap();

        assert_eq!(result, redis_value!({"foo": "baz", "bar": "foobar"}));
    }

    #[test]
    fn test_cluster_multi_shard_commands() {
        let cluster = TestClusterContext::new();

        let mut connection = cluster.connection();

        let res: String = connection
            .mset(&[("foo", "bar"), ("bar", "foo"), ("baz", "bazz")])
            .unwrap();
        assert_eq!(res, "OK");
        let res: Vec<String> = connection.mget(&["baz", "foo", "bar"]).unwrap();
        assert_eq!(res, vec!["bazz", "bar", "foo"]);
    }

    #[test]
    #[cfg(feature = "script")]
    fn test_cluster_script() {
        let cluster = TestClusterContext::new();
        let mut con = cluster.connection();

        let script = redis::Script::new(
            r#"
        redis.call("SET", KEYS[1], "1");
        redis.call("SET", KEYS[2], "2");
        return redis.call("MGET", KEYS[1], KEYS[2]);
    "#,
        );

        let rv = script.key("{x}a").key("{x}b").invoke(&mut con);
        assert_eq!(rv, Ok(("1".to_string(), "2".to_string())));
    }

    #[test]
    fn test_cluster_pipeline() {
        let cluster = TestClusterContext::new();
        cluster.wait_for_cluster_up();
        let mut con = cluster.connection();

        let resp = cluster_pipe()
            .cmd("SET")
            .arg("key_1")
            .arg(42)
            .query::<Vec<String>>(&mut con)
            .unwrap();

        assert_eq!(resp, vec!["OK".to_string()]);
    }

    #[test]
    fn test_cluster_pipeline_multiple_keys() {
        use redis::FromRedisValue;
        let cluster = TestClusterContext::new();
        cluster.wait_for_cluster_up();
        let mut con = cluster.connection();

        let resp = cluster_pipe()
            .cmd("HSET")
            .arg("hash_1")
            .arg("key_1")
            .arg("value_1")
            .cmd("ZADD")
            .arg("zset")
            .arg(1)
            .arg("zvalue_2")
            .query::<Vec<i64>>(&mut con)
            .unwrap();

        assert_eq!(resp, vec![1i64, 1i64]);

        let resp = cluster_pipe()
            .cmd("HGET")
            .arg("hash_1")
            .arg("key_1")
            .cmd("ZCARD")
            .arg("zset")
            .query::<Vec<redis::Value>>(&mut con)
            .unwrap();

        let resp_1: String = FromRedisValue::from_redis_value_ref(&resp[0]).unwrap();
        assert_eq!(resp_1, "value_1".to_string());

        let resp_2: usize = FromRedisValue::from_redis_value_ref(&resp[1]).unwrap();
        assert_eq!(resp_2, 1);
    }

    #[test]
    fn test_cluster_pipeline_invalid_command() {
        let cluster = TestClusterContext::new();
        cluster.wait_for_cluster_up();
        let mut con = cluster.connection();

        let err = cluster_pipe()
            .cmd("SET")
            .arg("foo")
            .arg(42)
            .ignore()
            .cmd(" SCRIPT kill ")
            .exec(&mut con)
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "This command cannot be safely routed in cluster mode - Client: Command 'SCRIPT KILL' can't be executed in a cluster pipeline."
        );

        let err = cluster_pipe().keys("*").exec(&mut con).unwrap_err();

        assert_eq!(
            err.to_string(),
            "This command cannot be safely routed in cluster mode - Client: Command 'KEYS' can't be executed in a cluster pipeline."
        );
    }

    #[test]
    fn test_cluster_pipeline_command_ordering() {
        let cluster = TestClusterContext::new();
        cluster.wait_for_cluster_up();
        let mut con = cluster.connection();
        let mut pipe = cluster_pipe();

        let mut queries = Vec::new();
        let mut expected = Vec::new();
        for i in 0..100 {
            queries.push(format!("foo{i}"));
            expected.push(format!("bar{i}"));
            pipe.set(&queries[i], &expected[i]).ignore();
        }
        pipe.exec(&mut con).unwrap();

        pipe.clear();
        for q in &queries {
            pipe.get(q);
        }

        let got = pipe.query::<Vec<String>>(&mut con).unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    #[ignore] // Flaky
    fn test_cluster_pipeline_ordering_with_improper_command() {
        let cluster = TestClusterContext::new();
        cluster.wait_for_cluster_up();
        let mut con = cluster.connection();
        let mut pipe = cluster_pipe();

        let mut queries = Vec::new();
        let mut expected = Vec::new();
        for i in 0..10 {
            if i == 5 {
                pipe.cmd("hset").arg("foo").ignore();
            } else {
                let query = format!("foo{i}");
                let r = format!("bar{i}");
                pipe.set(&query, &r).ignore();
                queries.push(query);
                expected.push(r);
            }
        }
        pipe.exec(&mut con).unwrap_err();

        std::thread::sleep(std::time::Duration::from_secs(5));

        pipe.clear();
        for q in &queries {
            pipe.get(q);
        }

        let got = pipe.query::<Vec<String>>(&mut con).unwrap();
        assert_eq!(got, expected);
    }

    #[test]
    fn test_cluster_can_connect_to_server_that_sends_cluster_slots_with_null_host_name() {
        let name =
            "test_cluster_can_connect_to_server_that_sends_cluster_slots_with_null_host_name";

        let MockEnv { mut connection, .. } = MockEnv::new(name, move |cmd: &[u8], _| {
            if is_connection_check(cmd) {
                Err(Ok(redis_value!(simple:"OK")))
            } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                Err(Ok(redis_value!([[0, 16383, [nil, 6379]]])))
            } else {
                Err(Ok(Value::Nil))
            }
        });

        let value = cmd("GET").arg("test").query::<Value>(&mut connection);

        assert_eq!(value, Ok(Value::Nil));
    }

    #[test]
    fn test_cluster_can_connect_to_server_that_sends_cluster_slots_with_partial_nodes_with_unknown_host_name()
     {
        let name = "test_cluster_can_connect_to_server_that_sends_cluster_slots_with_partial_nodes_with_unknown_host_name";

        let MockEnv { mut connection, .. } = MockEnv::new(name, move |cmd: &[u8], _| {
            if is_connection_check(cmd) {
                Err(Ok(redis_value!(simple:"OK")))
            } else if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                Err(Ok(redis_value!([
                    [0, 7000, [name, 6379]],
                    [7001, 16383, ["?", 6380]]
                ])))
            } else {
                Err(Ok(Value::Nil))
            }
        });

        let value = cmd("GET").arg("test").query::<Value>(&mut connection);
        assert_eq!(value, Ok(Value::Nil));
    }

    #[test]
    fn test_cluster_retries() {
        let name = "tryagain";

        let requests = atomic::AtomicUsize::new(0);
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(5),
            name,
            move |cmd: &[u8], _| {
                respond_startup(name, cmd)?;

                match requests.fetch_add(1, atomic::Ordering::SeqCst) {
                    0..=4 => Err(parse_redis_value(b"-TRYAGAIN mock\r\n")),
                    _ => Err(Ok(redis_value!("123"))),
                }
            },
        );

        let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

        assert_eq!(value, Ok(Some(123)));
    }

    #[test]
    fn test_cluster_exhaust_retries() {
        let name = "tryagain_exhaust_retries";

        let requests = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(2),
            name,
            {
                let requests = requests.clone();
                move |cmd: &[u8], _| {
                    respond_startup(name, cmd)?;
                    requests.fetch_add(1, atomic::Ordering::SeqCst);
                    Err(parse_redis_value(b"-TRYAGAIN mock\r\n"))
                }
            },
        );

        let result = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

        assert!(
            matches!(&result, Err(err) if err.kind() == ServerErrorKind::TryAgain.into()),
            "{result:?}",
        );
        assert_eq!(requests.load(atomic::Ordering::SeqCst), 3);
    }

    #[test]
    fn test_cluster_move_error_when_new_node_is_added() {
        let name = "rebuild_with_extra_nodes";

        let requests = atomic::AtomicUsize::new(0);
        let started = atomic::AtomicBool::new(false);
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            if !started.load(atomic::Ordering::SeqCst) {
                respond_startup(name, cmd)?;
            }
            started.store(true, atomic::Ordering::SeqCst);

            if is_connection_check(cmd) {
                return Err(Ok(redis_value!(simple:"OK")));
            }

            let i = requests.fetch_add(1, atomic::Ordering::SeqCst);

            match i {
                // Respond that the key exists on a node that does not yet have a connection:
                0 => Err(parse_redis_value(b"-MOVED 123\r\n")),
                // Respond with the new masters
                1 => Err(Ok(redis_value!([
                    [0, 1, [name, 6379]],
                    [2, 16383, [name, 6380]]
                ]))),
                _ => {
                    // Check that the correct node receives the request after rebuilding
                    assert_eq!(port, 6380);
                    Err(Ok(redis_value!("123")))
                }
            }
        });

        let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

        assert_eq!(value, Ok(Some(123)));
    }

    #[test]
    fn test_cluster_pipeline_moved_redirect() {
        // A `MOVED` returned for a pipelined sub-command is delivered
        // inline as `Ok(Value::ServerError(Moved))`, and must be followed per command
        //
        // Both commands target the same slot, so they are sent as one sub-pipeline to the initial owner,
        // which reports the slot has moved.
        let name = "test_cluster_pipeline_moved_redirect";
        let set_seen = Arc::new(atomic::AtomicBool::new(false));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, {
            let set_seen = set_seen.clone();
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;

                if port == 6379 {
                    // Initial slot owner reports the slot moved. For the pipelined sub-commands
                    // this reply is returned via `recv_response` (cmd == &[]).
                    return Err(parse_redis_value(
                        format!("-MOVED 123 {name}:6380\r\n").as_bytes(),
                    ));
                }

                assert_eq!(port, 6380);
                if contains_slice(cmd, b"GET") {
                    assert!(!set_seen.load(Ordering::SeqCst));
                    Err(Ok(redis_value!("old")))
                } else {
                    set_seen.store(true, Ordering::SeqCst);
                    Err(Ok(redis_value!(simple: "OK")))
                }
            }
        });

        let value = cluster_pipe()
            .cmd("GET")
            .arg("key1")
            .cmd("SET")
            .arg("key1")
            .arg("val1")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(value, Ok(vec!["old".to_string(), "OK".to_string()]));
        assert!(set_seen.load(Ordering::SeqCst));
        assert_eq!(take_pipeline_writes(name), vec![(6379, 2)]);
    }

    /// CRC16("x") % 16384, i.e. the slot every `{x}...` key routes to. Owned by
    /// 6379 under `respond_startup`.
    const X_TAG_SLOT: u16 = 16287;

    #[test]
    fn test_cluster_pipeline_moved_is_retried_as_one_pipeline() {
        // The commands displaced by a reshard are re-sent as a new pipeline, so a
        // pipeline of N commands that all move costs one extra round trip rather
        // than N single-command requests.
        let name = "test_cluster_pipeline_moved_is_retried_as_one_pipeline";

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            respond_startup(name, cmd)?;

            match port {
                // The old owner reports the whole slot moved.
                6379 => Err(parse_redis_value(
                    format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                )),
                6380 if contains_slice(cmd, b"key1") => Err(Ok(redis_value!("val1"))),
                6380 if contains_slice(cmd, b"key2") => Err(Ok(redis_value!("val2"))),
                6380 if contains_slice(cmd, b"key3") => Err(Ok(redis_value!("val3"))),
                _ => panic!("Wrong node"),
            }
        });

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .cmd("GET")
            .arg("{x}key2")
            .cmd("GET")
            .arg("{x}key3")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(
            value,
            Ok(vec![
                "val1".to_string(),
                "val2".to_string(),
                "val3".to_string()
            ])
        );
        // One three-command write to the old owner, then one three-command write
        // to the new one. Retrying command by command would leave the retries out
        // of this list entirely, since those go through `req_command`.
        assert_eq!(take_pipeline_writes(name), vec![(6379, 3), (6380, 3)]);
    }

    #[test]
    fn test_cluster_pipeline_mutations_keep_single_command_retries() {
        let name = "test_cluster_pipeline_mutations_keep_single_command_retries";
        let target_calls = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, {
            let target_calls = target_calls.clone();
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;

                match port {
                    6379 => Err(parse_redis_value(
                        format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                    )),
                    6380 => {
                        assert!(contains_slice(cmd, b"SET"));
                        target_calls.fetch_add(1, Ordering::SeqCst);
                        Err(Ok(redis_value!(simple: "OK")))
                    }
                    _ => panic!("Wrong node"),
                }
            }
        });

        let value = cluster_pipe()
            .cmd("SET")
            .arg("{x}key1")
            .arg("val1")
            .cmd("SET")
            .arg("{x}key2")
            .arg("val2")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(value, Ok(vec!["OK".to_string(), "OK".to_string()]));
        assert_eq!(target_calls.load(Ordering::SeqCst), 2);
        // Only the initial request is pipelined. Mutations retain the legacy
        // single-command retry path.
        assert_eq!(take_pipeline_writes(name), vec![(6379, 2)]);
    }

    #[test]
    fn test_cluster_pipeline_conflicting_moved_hints_refresh_the_slot_map() {
        let name = "test_cluster_pipeline_conflicting_moved_hints_refresh_the_slot_map";
        let topology_reads = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, {
            let topology_reads = topology_reads.clone();
            move |cmd: &[u8], port| {
                if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                    topology_reads.fetch_add(1, Ordering::SeqCst);
                }
                respond_startup(name, cmd)?;

                match port {
                    6379 if contains_slice(cmd, b"key1") => Err(parse_redis_value(
                        format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                    )),
                    6379 => Err(parse_redis_value(
                        format!("-MOVED {X_TAG_SLOT} {name}:6381\r\n").as_bytes(),
                    )),
                    6380 => Err(Ok(redis_value!("one"))),
                    6381 => Err(Ok(redis_value!("two"))),
                    _ => panic!("Wrong node: {port}"),
                }
            }
        });
        let startup_topology_reads = topology_reads.load(Ordering::SeqCst);

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .cmd("GET")
            .arg("{x}key2")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(value, Ok(vec!["one".to_string(), "two".to_string()]));
        // Conflicting authoritative owners for one slot require one additional
        // unthrottled refresh instead of choosing whichever hint was processed last.
        assert_eq!(
            topology_reads.load(Ordering::SeqCst),
            startup_topology_reads + 1
        );

        let mut writes = take_pipeline_writes(name);
        assert_eq!(writes.remove(0), (6379, 2));
        writes.sort();
        assert_eq!(writes, vec![(6380, 1), (6381, 1)]);
    }

    #[test]
    fn test_cluster_pipeline_retries_only_the_commands_that_failed() {
        // A pipeline that straddles two nodes and gets a redirect from one of them
        // re-sends only that node's commands.
        let name = "test_cluster_pipeline_retries_only_the_commands_that_failed";

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            respond_startup_two_nodes(name, cmd)?;

            match port {
                // 6379 owns `{lo}a`'s slot and reports it moved to 6380.
                6379 => Err(parse_redis_value(
                    format!("-MOVED 4878 {name}:6380\r\n").as_bytes(),
                )),
                // 6380 owns `{hi}a` outright, and serves `{lo}a` after the redirect.
                6380 if contains_slice(cmd, b"{hi}a") => Err(Ok(redis_value!("hi"))),
                6380 => Err(Ok(redis_value!("lo"))),
                _ => panic!("Wrong node"),
            }
        });

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{lo}a")
            .cmd("GET")
            .arg("{hi}a")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(value, Ok(vec!["lo".to_string(), "hi".to_string()]));

        let mut writes = take_pipeline_writes(name);
        // The retry is the last write: one command, the only one that moved.
        assert_eq!(writes.pop(), Some((6380, 1)));
        // The initial fan-out is one command per node, in unspecified node order.
        writes.sort();
        assert_eq!(writes, vec![(6379, 1), (6380, 1)]);
    }

    #[test]
    fn test_cluster_pipeline_recovers_retry_send_failure_for_pending_commands() {
        let name = "test_cluster_pipeline_recovers_retry_send_failure_for_pending_commands";
        set_pipeline_send_results(
            name,
            vec![
                Ok(()),
                Err(RedisError::from(std::io::Error::from(
                    std::io::ErrorKind::BrokenPipe,
                ))),
                Ok(()),
            ],
        );

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            respond_startup(name, cmd)?;

            match port {
                6379 => Err(parse_redis_value(
                    format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                )),
                6380 if contains_slice(cmd, b"key1") => Err(Ok(redis_value!("one"))),
                6380 if contains_slice(cmd, b"key2") => Err(Ok(redis_value!("two"))),
                _ => panic!("Wrong node or command: port={port}, cmd={cmd:?}"),
            }
        });

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .cmd("GET")
            .arg("{x}key2")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(value, Ok(vec!["one".to_string(), "two".to_string()]));
        // The failed write is retried internally with the same pending subset.
        // Returning its I/O error would make redis-lightning replay the original
        // pipeline instead.
        assert_eq!(
            take_pipeline_writes(name),
            vec![(6379, 2), (6380, 2), (6380, 2)]
        );
    }

    #[test]
    fn test_cluster_pipeline_recovers_retry_receive_failure_for_unresolved_commands() {
        let name = "test_cluster_pipeline_recovers_retry_receive_failure_for_unresolved_commands";
        let key2_responses = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, {
            let key2_responses = key2_responses.clone();
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;

                match port {
                    6379 if contains_slice(cmd, b"stable") => Err(Ok(redis_value!("stable"))),
                    6379 => Err(parse_redis_value(
                        format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                    )),
                    6380 if contains_slice(cmd, b"key1") => Err(Ok(redis_value!("one"))),
                    6380 if contains_slice(cmd, b"key2") => {
                        if key2_responses.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(Err(RedisError::from(std::io::Error::from(
                                std::io::ErrorKind::ConnectionReset,
                            ))))
                        } else {
                            Err(Ok(redis_value!("two")))
                        }
                    }
                    6380 if contains_slice(cmd, b"key3") => Err(Ok(redis_value!("three"))),
                    _ => panic!("Wrong node or command: port={port}, cmd={cmd:?}"),
                }
            }
        });

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}stable")
            .cmd("GET")
            .arg("{x}key1")
            .cmd("GET")
            .arg("{x}key2")
            .cmd("GET")
            .arg("{x}key3")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(
            value,
            Ok(vec![
                "stable".to_string(),
                "one".to_string(),
                "two".to_string(),
                "three".to_string(),
            ])
        );
        // The stable command and key1 already have responses. Only the command
        // whose response failed and the unread command behind it are replayed.
        assert_eq!(
            take_pipeline_writes(name),
            vec![(6379, 4), (6380, 3), (6380, 2)]
        );
    }

    #[test]
    fn test_cluster_pipeline_returns_ambiguous_mutation_failure_without_retrying() {
        let name = "test_cluster_pipeline_returns_ambiguous_mutation_failure_without_retrying";
        let increment_responses = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, {
            let increment_responses = increment_responses.clone();
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;

                assert_eq!(port, 6379);
                assert!(contains_slice(cmd, b"INCR"));
                if increment_responses.fetch_add(1, Ordering::SeqCst) == 0 {
                    Err(Err(RedisError::from(std::io::Error::from(
                        std::io::ErrorKind::ConnectionReset,
                    ))))
                } else {
                    Err(Ok(redis_value!(2)))
                }
            }
        });

        let result = cluster_pipe()
            .cmd("INCR")
            .arg("{x}counter")
            .query::<Vec<i64>>(&mut connection);

        assert!(
            matches!(&result, Err(err) if err.is_io_error()),
            "{result:?}"
        );
        assert_eq!(increment_responses.load(Ordering::SeqCst), 1);
        assert_eq!(take_pipeline_writes(name), vec![(6379, 1)]);
    }

    #[test]
    fn test_cluster_pipeline_keeps_the_connection_across_a_receive_timeout() {
        // A read timeout leaves the socket alive with the abandoned suffix's
        // frames still in flight. Those frames can be accounted for and
        // discarded, so the connection must be realigned and reused — replacing
        // it on every timeout turns a reshard's simultaneous timeouts into a
        // reconnect stampede (observed as ~6,400 NewConnections/run and 20 s
        // connect stalls in the 2026-08-31 reshard benchmark).
        let name = "test_cluster_pipeline_keeps_the_connection_across_a_receive_timeout";
        let key2_responses = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, {
            let key2_responses = key2_responses.clone();
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;

                match port {
                    6379 if contains_slice(cmd, b"key1") => Err(Ok(redis_value!("one"))),
                    6379 if contains_slice(cmd, b"key2") => {
                        if key2_responses.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(Err(RedisError::from(std::io::Error::from(
                                std::io::ErrorKind::TimedOut,
                            ))))
                        } else {
                            Err(Ok(redis_value!("two")))
                        }
                    }
                    6379 if contains_slice(cmd, b"key3") => Err(Ok(redis_value!("three"))),
                    _ => panic!("Wrong node or command: port={port}, cmd={cmd:?}"),
                }
            }
        });
        // Discard the connects made while the client started up.
        let _ = take_connects(name);

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .cmd("GET")
            .arg("{x}key2")
            .cmd("GET")
            .arg("{x}key3")
            .query::<Vec<String>>(&mut connection);

        // Correct results prove the realignment: on a reused-but-unaccounted
        // socket the retry would read key3's buffered answer as key2's.
        assert_eq!(
            value,
            Ok(vec![
                "one".to_string(),
                "two".to_string(),
                "three".to_string(),
            ])
        );
        assert_eq!(take_pipeline_writes(name), vec![(6379, 3), (6379, 2)]);
        // The retry must reuse the timed-out connection, not replace it.
        assert_eq!(take_connects(name), Vec::<u16>::new());
    }

    #[test]
    fn test_cluster_pipeline_ask_redirect_asks_inside_the_retry_pipeline() {
        // `ASKING` only covers the command that immediately follows it, so a
        // retried pipeline has to carry one per redirected command rather than one
        // per pipe.
        let name = "test_cluster_pipeline_ask_redirect_asks_inside_the_retry_pipeline";
        let asking = Arc::new(atomic::AtomicUsize::new(0));
        let preceded_by_asking = Arc::new(atomic::AtomicBool::new(false));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, {
            let asking = asking.clone();
            let preceded_by_asking = preceded_by_asking.clone();
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;

                match port {
                    // The slot is migrating away from 6379, key by key.
                    6379 => Err(parse_redis_value(
                        format!("-ASK {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                    )),
                    6380 => {
                        if contains_slice(cmd, b"ASKING") {
                            asking.fetch_add(1, Ordering::SeqCst);
                            preceded_by_asking.store(true, Ordering::SeqCst);
                            return Err(Ok(redis_value!(simple: "OK")));
                        }
                        // Each redirected command must arrive right behind its own
                        // ASKING, not behind one the previous command consumed.
                        assert!(
                            preceded_by_asking.swap(false, Ordering::SeqCst),
                            "{cmd:?} reached the migration target without an ASKING"
                        );
                        Err(Ok(redis_value!("migrated")))
                    }
                    _ => panic!("Wrong node"),
                }
            }
        });

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .cmd("GET")
            .arg("{x}key2")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(
            value,
            Ok(vec!["migrated".to_string(), "migrated".to_string()])
        );
        assert_eq!(asking.load(Ordering::SeqCst), 2);
        // The retry pipe carries two commands and the two `ASKING`s in front of them.
        assert_eq!(take_pipeline_writes(name), vec![(6379, 2), (6380, 4)]);

        // An ASK does not transfer ownership of the slot, so the next pipeline
        // still starts at 6379.
        let _ = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .query::<Vec<String>>(&mut connection);
        assert_eq!(take_pipeline_writes(name).first(), Some(&(6379, 1)));
    }

    #[test]
    fn test_cluster_pipeline_returns_asking_error_after_draining_guarded_response() {
        let name = "test_cluster_pipeline_returns_asking_error_after_draining_guarded_response";
        let guarded_responses = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(1),
            name,
            {
                let guarded_responses = guarded_responses.clone();
                move |cmd: &[u8], port| {
                    respond_startup(name, cmd)?;

                    match port {
                        6379 => Err(parse_redis_value(
                            format!("-ASK {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                        )),
                        6380 if contains_slice(cmd, b"ASKING") => {
                            Err(parse_redis_value(b"-NOPERM ASKING is forbidden\r\n"))
                        }
                        6380 => {
                            guarded_responses.fetch_add(1, Ordering::SeqCst);
                            // This is the response behind the failed ASKING. It
                            // still has to be consumed to preserve framing, but
                            // must not replace the more informative NOPERM.
                            Err(parse_redis_value(
                                format!("-ASK {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                            ))
                        }
                        _ => panic!("Wrong node: {port}"),
                    }
                }
            },
        );

        let result = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .query::<Vec<String>>(&mut connection);

        assert!(
            matches!(&result, Err(err) if err.kind() == ServerErrorKind::NoPerm.into()),
            "{result:?}",
        );
        assert_eq!(guarded_responses.load(Ordering::SeqCst), 1);
        assert_eq!(take_pipeline_writes(name), vec![(6379, 1), (6380, 2)]);
    }

    #[test]
    fn test_cluster_pipeline_does_not_double_enqueue_an_ask_guarded_command() {
        // A retryable ASKING error followed by a transport failure on the guarded
        // read used to enqueue the same command twice — once for the ASKING error
        // and once in the broken drain's suffix — so the next round executed it
        // twice. Non-idempotent commands must run exactly once per round.
        let name = "test_cluster_pipeline_does_not_double_enqueue_an_ask_guarded_command";
        let asking_calls = Arc::new(atomic::AtomicUsize::new(0));
        let guarded_calls = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, {
            let asking_calls = asking_calls.clone();
            let guarded_calls = guarded_calls.clone();
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;

                match port {
                    6379 => Err(parse_redis_value(
                        format!("-ASK {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                    )),
                    6380 if contains_slice(cmd, b"ASKING") => {
                        if asking_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(parse_redis_value(b"-TRYAGAIN try again later\r\n"))
                        } else {
                            Err(Ok(Value::SimpleString("OK".into())))
                        }
                    }
                    6380 => {
                        if guarded_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                            Err(Err(RedisError::from(std::io::Error::from(
                                std::io::ErrorKind::ConnectionReset,
                            ))))
                        } else {
                            Err(Ok(redis_value!("one")))
                        }
                    }
                    _ => panic!("Wrong node: {port}"),
                }
            }
        });

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(value, Ok(vec!["one".to_string()]));
        // The third write must carry one ASKING + one command, not two of each.
        assert_eq!(
            take_pipeline_writes(name),
            vec![(6379, 1), (6380, 2), (6380, 2)]
        );
    }

    #[test]
    fn test_cluster_pipeline_unpins_a_redirect_whose_target_cannot_connect() {
        // A MOVED can name a node the client cannot reach (e.g. one that left the
        // cluster mid-reshard). Once reconnecting to it fails, the pin must not
        // stick for the whole retry budget; the command falls back to the slot
        // map, which the refresh has meanwhile corrected.
        let name = "test_cluster_pipeline_unpins_a_redirect_whose_target_cannot_connect";
        set_connect_errors(name, [6380]);
        let key1_reads = Arc::new(atomic::AtomicUsize::new(0));
        let topology_reads = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(3)
                // The connection failure must bypass the MOVED refresh limiter.
                .min_slot_refresh_interval(std::time::Duration::from_secs(3600)),
            name,
            {
                let key1_reads = key1_reads.clone();
                let topology_reads = topology_reads.clone();
                move |cmd: &[u8], port| {
                    if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                        topology_reads.fetch_add(1, Ordering::SeqCst);
                    }
                    respond_startup(name, cmd)?;

                    match port {
                        6379 => {
                            if key1_reads.fetch_add(1, Ordering::SeqCst) == 0 {
                                Err(parse_redis_value(
                                    format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                                ))
                            } else {
                                Err(Ok(redis_value!("one")))
                            }
                        }
                        _ => panic!("Wrong node: {port}"),
                    }
                }
            },
        );
        let startup_topology_reads = topology_reads.load(Ordering::SeqCst);

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(value, Ok(vec!["one".to_string()]));
        assert_eq!(
            topology_reads.load(Ordering::SeqCst),
            startup_topology_reads + 1
        );
        // The round pinned to 6380 never gets as far as a write (connect fails);
        // the round after routes through the slot map back to 6379.
        assert_eq!(take_pipeline_writes(name), vec![(6379, 1), (6379, 1)]);
    }

    #[test]
    fn test_cluster_pipeline_unpins_redirects_already_in_the_delayed_queue() {
        // The slow command is already backing off with a 6380 redirect when the
        // fast command discovers that 6380 is unreachable. Recovery must unpin
        // both commands, including the one retained by the outer scheduler.
        let name = "test_cluster_pipeline_unpins_redirects_already_in_the_delayed_queue";
        let slow_on_initial = Arc::new(atomic::AtomicUsize::new(0));
        let fast_on_initial = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(6)
                .min_retry_wait(100)
                .max_retry_wait(101),
            name,
            {
                let slow_on_initial = slow_on_initial.clone();
                let fast_on_initial = fast_on_initial.clone();
                move |cmd: &[u8], port| {
                    respond_startup(name, cmd)?;

                    if contains_slice(cmd, b"slow") {
                        return match port {
                            6379 if slow_on_initial.fetch_add(1, Ordering::SeqCst) == 0 => {
                                Err(parse_redis_value(
                                    format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                                ))
                            }
                            6379 => Err(Ok(redis_value!("slow-ok"))),
                            6380 => Err(parse_redis_value(b"-TRYAGAIN wait for it\r\n")),
                            _ => panic!("Wrong node for slow command: {port}"),
                        };
                    }

                    match port {
                        6379 if fast_on_initial.fetch_add(1, Ordering::SeqCst) == 0 => {
                            Err(parse_redis_value(
                                format!("-MOVED {X_TAG_SLOT} {name}:6381\r\n").as_bytes(),
                            ))
                        }
                        6379 => Err(Ok(redis_value!("fast-ok"))),
                        6381 => Err(parse_redis_value(
                            format!("-MOVED {X_TAG_SLOT} {name}:6382\r\n").as_bytes(),
                        )),
                        6382 => Err(parse_redis_value(
                            format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                        )),
                        6380 => {
                            // Drop the live connection, then make recovery fail.
                            set_connect_errors(name, [6380]);
                            Err(Err(RedisError::from(std::io::Error::from(
                                std::io::ErrorKind::ConnectionReset,
                            ))))
                        }
                        _ => panic!("Wrong node for fast command: {port}"),
                    }
                }
            },
        );
        // Ignore the initial node connection; the assertion below starts with
        // the retry pipelines.
        let _ = take_connect_attempts(name);

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}slow")
            .cmd("GET")
            .arg("{x}fast")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(
            value,
            Ok(vec!["slow-ok".to_string(), "fast-ok".to_string()])
        );
        let attempts = take_connect_attempts(name);
        // 6380 is attempted once for the first redirected pipeline and once by
        // recovery. A later attempt means the delayed command kept its stale pin.
        assert_eq!(attempts.iter().filter(|&&port| port == 6380).count(), 2);
    }

    #[test]
    fn test_cluster_pipeline_routes_an_uncovered_slot_to_a_random_node() {
        // A refresh that races a reshard can return a slot map with a gap. A
        // command in the gap must bounce off some node and follow the MOVED it
        // gets back — like the single-command path — rather than failing the
        // whole pipeline with "Missing slot coverage".
        let name = "test_cluster_pipeline_routes_an_uncovered_slot_to_a_random_node";

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            // The advertised topology covers only slots 0..8191; {x} hashes to
            // slot 16287, which no node claims.
            respond_startup_with_replica_using_config(
                name,
                cmd,
                Some(vec![MockSlotRange {
                    primary_port: 6379,
                    replica_ports: vec![],
                    slot_range: (0..8191),
                }]),
            )?;

            match port {
                6379 => Err(parse_redis_value(
                    format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                )),
                6380 => Err(Ok(redis_value!("one"))),
                _ => panic!("Wrong node: {port}"),
            }
        });

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(value, Ok(vec!["one".to_string()]));
        assert_eq!(take_pipeline_writes(name), vec![(6379, 1), (6380, 1)]);
    }

    #[test]
    fn test_cluster_pipeline_survives_a_failed_rate_limited_refresh() {
        // The refresh that follows a batch of MOVED hints is an optimisation, not
        // the recovery mechanism: the hints and pinned redirects already route
        // the retry. Its failure — likely on exactly the busy cluster that
        // produced the redirects — must not fail the pipeline.
        let name = "test_cluster_pipeline_survives_a_failed_rate_limited_refresh";
        let moved_served = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(2)
                .min_slot_refresh_interval(std::time::Duration::ZERO),
            name,
            {
                let moved_served = moved_served.clone();
                move |cmd: &[u8], port| {
                    if moved_served.load(Ordering::SeqCst) > 0
                        && contains_slice(cmd, b"CLUSTER")
                        && contains_slice(cmd, b"SLOTS")
                    {
                        return Err(Err(RedisError::from(std::io::Error::other(
                            "topology refresh unavailable",
                        ))));
                    }
                    respond_startup(name, cmd)?;

                    match port {
                        6379 => {
                            moved_served.fetch_add(1, Ordering::SeqCst);
                            Err(parse_redis_value(
                                format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                            ))
                        }
                        6380 => Err(Ok(redis_value!("one"))),
                        _ => panic!("Wrong node: {port}"),
                    }
                }
            },
        );

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(value, Ok(vec!["one".to_string()]));
        assert_eq!(take_pipeline_writes(name), vec![(6379, 1), (6380, 1)]);
    }

    #[test]
    fn test_cluster_pipeline_keeps_a_final_round_moved_hint() {
        let name = "test_cluster_pipeline_keeps_a_final_round_moved_hint";

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .min_slot_refresh_interval(std::time::Duration::from_secs(3600)),
            name,
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;

                match port {
                    6379 => Err(parse_redis_value(
                        format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                    )),
                    6380 => Err(Ok(redis_value!("one"))),
                    _ => panic!("Wrong node: {port}"),
                }
            },
        );

        let first = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .query::<Vec<String>>(&mut connection);
        assert!(matches!(&first, Err(err) if err.kind() == ServerErrorKind::Moved.into()));

        let second = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .query::<Vec<String>>(&mut connection);
        assert_eq!(second, Ok(vec!["one".to_string()]));
        assert_eq!(take_pipeline_writes(name), vec![(6379, 1), (6380, 1)]);
    }

    #[test]
    fn test_cluster_pipeline_omits_conflicting_final_round_moved_hints() {
        let name = "test_cluster_pipeline_omits_conflicting_final_round_moved_hints";

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(0),
            name,
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;

                match port {
                    6379 if contains_slice(cmd, b"key1") => Err(parse_redis_value(
                        format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                    )),
                    6379 if contains_slice(cmd, b"key2") => Err(parse_redis_value(
                        format!("-MOVED {X_TAG_SLOT} {name}:6381\r\n").as_bytes(),
                    )),
                    6379 => Err(Ok(redis_value!("three"))),
                    _ => panic!("Conflicting hint poisoned the slot map: {port}"),
                }
            },
        );

        let first = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .cmd("GET")
            .arg("{x}key2")
            .query::<Vec<String>>(&mut connection);
        assert!(matches!(&first, Err(err) if err.kind() == ServerErrorKind::Moved.into()));

        let second = cluster_pipe()
            .cmd("GET")
            .arg("{x}key3")
            .query::<Vec<String>>(&mut connection);
        assert_eq!(second, Ok(vec!["three".to_string()]));
        assert_eq!(take_pipeline_writes(name), vec![(6379, 2), (6379, 1)]);
    }

    #[test]
    fn test_cluster_pipeline_prioritizes_a_terminal_error_on_give_up() {
        let name = "test_cluster_pipeline_prioritizes_a_terminal_error_on_give_up";

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(0),
            name,
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;

                match port {
                    6379 if contains_slice(cmd, b"key1") => Err(parse_redis_value(
                        format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                    )),
                    6379 => Err(Err(RedisError::from((
                        ErrorKind::Client,
                        "terminal pipeline failure",
                    )))),
                    _ => panic!("Wrong node: {port}"),
                }
            },
        );

        let result = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .cmd("GET")
            .arg("{x}key2")
            .query::<Vec<String>>(&mut connection);

        assert!(matches!(&result, Err(err) if err.kind() == ErrorKind::Client));
        assert_eq!(take_pipeline_writes(name), vec![(6379, 2)]);
    }

    #[test]
    fn test_cluster_pipeline_wait_and_retry_does_not_block_other_retries() {
        // A TRYAGAIN backs off on its own clock (~1.28s at the default floor).
        // A MOVED in the same batch must retry immediately instead of waiting
        // out the other command's backoff, and the TRYAGAIN must still wait its
        // full delay before its own retry.
        let name = "test_cluster_pipeline_wait_and_retry_does_not_block_other_retries";

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            respond_startup(name, cmd)?;

            match port {
                6379 if contains_slice(cmd, b"slow") => {
                    Err(parse_redis_value(b"-TRYAGAIN wait for it\r\n"))
                }
                // The MOVED both redirects the fast command and teaches the
                // slot map, so the slow command's delayed retry also routes to
                // 6380 and succeeds there.
                6379 => Err(parse_redis_value(
                    format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                )),
                6380 if contains_slice(cmd, b"slow") => Err(Ok(redis_value!("slow-ok"))),
                6380 => Err(Ok(redis_value!("moved-ok"))),
                _ => panic!("Wrong node: {port}"),
            }
        });
        let _ = take_pipeline_write_times(name);

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}slow")
            .cmd("GET")
            .arg("{x}moved")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(
            value,
            Ok(vec!["slow-ok".to_string(), "moved-ok".to_string()])
        );
        // Three writes: the original pair, the immediate MOVED retry alone, and
        // the TRYAGAIN's retry alone after its backoff. The blocking behaviour
        // produced two writes, with the MOVED retry held inside the slept round.
        assert_eq!(
            take_pipeline_writes(name),
            vec![(6379, 2), (6380, 1), (6380, 1)]
        );
        let times = take_pipeline_write_times(name);
        assert!(
            times[1] - times[0] < std::time::Duration::from_millis(600),
            "MOVED retry was delayed {:?} — it inherited the TRYAGAIN backoff",
            times[1] - times[0],
        );
        assert!(
            times[2] - times[0] >= std::time::Duration::from_millis(1200),
            "TRYAGAIN retried after only {:?} — backoff not honoured",
            times[2] - times[0],
        );
    }

    #[test]
    fn test_cluster_pipeline_delayed_commands_keep_their_own_retry_budget() {
        // The fast command uses both of its retries while the slow command is
        // backing off. The slow command must still receive both configured
        // retries rather than inheriting the fast command's exhausted budget.
        let name = "test_cluster_pipeline_delayed_commands_keep_their_own_retry_budget";
        let slow_calls = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(2)
                .min_retry_wait(100)
                .max_retry_wait(101),
            name,
            {
                let slow_calls = slow_calls.clone();
                move |cmd: &[u8], port| {
                    respond_startup(name, cmd)?;

                    if contains_slice(cmd, b"slow") {
                        return if slow_calls.fetch_add(1, Ordering::SeqCst) < 2 {
                            Err(parse_redis_value(b"-TRYAGAIN wait for it\r\n"))
                        } else {
                            Err(Ok(redis_value!("slow-ok")))
                        };
                    }
                    match port {
                        6379 => Err(parse_redis_value(
                            format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                        )),
                        6380 => Err(parse_redis_value(
                            format!("-MOVED {X_TAG_SLOT} {name}:6381\r\n").as_bytes(),
                        )),
                        6381 => Err(Ok(redis_value!("fast-ok"))),
                        _ => panic!("Wrong node: {port}"),
                    }
                }
            },
        );

        let value = cluster_pipe()
            .cmd("GET")
            .arg("{x}slow")
            .cmd("GET")
            .arg("{x}fast")
            .query::<Vec<String>>(&mut connection);

        assert_eq!(
            value,
            Ok(vec!["slow-ok".to_string(), "fast-ok".to_string()])
        );
        assert_eq!(slow_calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            take_pipeline_writes(name),
            vec![(6379, 2), (6380, 1), (6381, 1), (6381, 1), (6381, 1),]
        );
    }

    #[test]
    fn test_cluster_pipeline_gives_up_after_the_configured_retries() {
        // A redirect the client can never satisfy is retried as a pipeline the
        // configured number of times, then reported.
        let name = "test_cluster_pipeline_gives_up_after_the_configured_retries";

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(2),
            name,
            move |cmd: &[u8], _| {
                respond_startup(name, cmd)?;
                // A MOVED with no address to redirect to: nothing to learn, and
                // nowhere else to send the commands.
                Err(parse_redis_value(
                    format!("-MOVED {X_TAG_SLOT}\r\n").as_bytes(),
                ))
            },
        );

        let result = cluster_pipe()
            .cmd("GET")
            .arg("{x}key1")
            .cmd("GET")
            .arg("{x}key2")
            .query::<Vec<String>>(&mut connection);

        assert!(
            matches!(&result, Err(err) if err.kind() == ServerErrorKind::Moved.into()),
            "{result:?}",
        );
        // The initial attempt plus two retries, each one still a single pipeline.
        assert_eq!(
            take_pipeline_writes(name),
            vec![(6379, 2), (6379, 2), (6379, 2)]
        );
    }

    /// CRC16("test") % 16384, i.e. the slot `GET test` routes to. Owned by 6379
    /// under `respond_startup_two_nodes`.
    const TEST_KEY_SLOT: u16 = 6918;

    #[test]
    fn test_cluster_moved_does_not_record_an_unreachable_target() {
        let name = "moved_does_not_record_an_unreachable_target";
        set_connect_errors(name, [6380]);
        let requests_to_6379 = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, {
            let requests_to_6379 = requests_to_6379.clone();
            move |cmd: &[u8], port| {
                respond_startup(name, cmd)?;
                match port {
                    6379 if requests_to_6379.fetch_add(1, Ordering::SeqCst) == 0 => {
                        Err(parse_redis_value(
                            format!("-MOVED {X_TAG_SLOT} {name}:6380\r\n").as_bytes(),
                        ))
                    }
                    6379 => Err(Ok(redis_value!("one"))),
                    _ => panic!("Wrong node: {port}"),
                }
            }
        });
        let _ = take_connect_attempts(name);

        let result = cmd("GET").arg("{x}key1").query::<String>(&mut connection);
        assert_matches!(result, Err(err) if err.kind() == ErrorKind::Io);
        assert_eq!(take_connect_attempts(name), vec![6380]);

        assert_eq!(
            cmd("GET").arg("{x}key1").query::<String>(&mut connection),
            Ok("one".to_string())
        );
        assert!(take_connect_attempts(name).is_empty());
        assert_eq!(requests_to_6379.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_cluster_moved_updates_slot_map_without_a_full_refresh() {
        let name = "moved_updates_slot_map";
        let slot_refreshes = Arc::new(atomic::AtomicUsize::new(0));
        let moveds = Arc::new(atomic::AtomicUsize::new(0));
        let requests_to_6380 = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                // Long enough that only the connection-time refresh may run, so
                // anything the client still routes correctly it learned from the
                // MOVED itself rather than from a refetched map.
                .min_slot_refresh_interval(std::time::Duration::from_secs(3600)),
            name,
            {
                let slot_refreshes = slot_refreshes.clone();
                let moveds = moveds.clone();
                let requests_to_6380 = requests_to_6380.clone();
                move |cmd: &[u8], port| {
                    if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                        slot_refreshes.fetch_add(1, Ordering::SeqCst);
                    }
                    respond_startup_two_nodes(name, cmd)?;
                    match port {
                        // 6379 still believes it owns the slot and always redirects.
                        6379 => {
                            moveds.fetch_add(1, Ordering::SeqCst);
                            Err(parse_redis_value(
                                format!("-MOVED {TEST_KEY_SLOT} {name}:6380\r\n").as_bytes(),
                            ))
                        }
                        6380 => {
                            requests_to_6380.fetch_add(1, Ordering::SeqCst);
                            Err(Ok(redis_value!("123")))
                        }
                        _ => panic!("Wrong node"),
                    }
                }
            },
        );

        let refreshes_after_connect = slot_refreshes.load(Ordering::SeqCst);

        assert_eq!(
            cmd("GET").arg("test").query::<Option<i32>>(&mut connection),
            Ok(Some(123))
        );
        assert_eq!(moveds.load(Ordering::SeqCst), 1);

        // Every later request for the same slot must go straight to the new owner.
        for _ in 0..5 {
            assert_eq!(
                cmd("GET").arg("test").query::<Option<i32>>(&mut connection),
                Ok(Some(123))
            );
        }
        assert_eq!(
            moveds.load(Ordering::SeqCst),
            1,
            "slot map did not learn the new owner from the MOVED"
        );
        assert_eq!(requests_to_6380.load(Ordering::SeqCst), 6);
        assert_eq!(
            slot_refreshes.load(Ordering::SeqCst),
            refreshes_after_connect,
            "MOVED triggered a full CLUSTER SLOTS despite the refresh interval"
        );
    }

    #[test]
    fn test_cluster_ask_does_not_update_slot_map() {
        let name = "ask_does_not_update_slot_map";
        let asks = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .min_slot_refresh_interval(std::time::Duration::from_secs(3600)),
            name,
            {
                let asks = asks.clone();
                move |cmd: &[u8], port| {
                    respond_startup_two_nodes(name, cmd)?;
                    match port {
                        6379 => {
                            asks.fetch_add(1, Ordering::SeqCst);
                            Err(parse_redis_value(
                                format!("-ASK {TEST_KEY_SLOT} {name}:6380\r\n").as_bytes(),
                            ))
                        }
                        6380 => {
                            if contains_slice(cmd, b"ASKING") {
                                Err(Ok(Value::Okay))
                            } else {
                                Err(Ok(redis_value!("123")))
                            }
                        }
                        _ => panic!("Wrong node"),
                    }
                }
            },
        );

        // An ASK is a one-request instruction about a slot that is still owned by
        // the node issuing it, so it must not be recorded in the slot map. If it
        // were, this slot's not-yet-migrated keys would be sent to a node that
        // does not serve them. Every request must therefore still start at 6379.
        for i in 1..=5 {
            assert_eq!(
                cmd("GET").arg("test").query::<Option<i32>>(&mut connection),
                Ok(Some(123))
            );
            assert_eq!(
                asks.load(Ordering::SeqCst),
                i,
                "an ASK redirect was written into the slot map"
            );
        }
    }

    #[test]
    fn test_cluster_moved_without_an_address_still_forces_a_refresh() {
        let name = "moved_without_address";
        let slot_refreshes = Arc::new(atomic::AtomicUsize::new(0));
        let started = Arc::new(atomic::AtomicBool::new(false));
        let requests = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .min_slot_refresh_interval(std::time::Duration::from_secs(3600)),
            name,
            {
                let slot_refreshes = slot_refreshes.clone();
                let started = started.clone();
                let requests = requests.clone();
                move |cmd: &[u8], port| {
                    if !started.load(Ordering::SeqCst) {
                        respond_startup(name, cmd)?;
                    }
                    started.store(true, Ordering::SeqCst);
                    if is_connection_check(cmd) {
                        return Err(Ok(redis_value!(simple:"OK")));
                    }
                    match requests.fetch_add(1, Ordering::SeqCst) {
                        // A MOVED carrying no address: nothing to record, and no node
                        // to redirect to.
                        0 => Err(parse_redis_value(b"-MOVED 123\r\n")),
                        1 => {
                            slot_refreshes.fetch_add(1, Ordering::SeqCst);
                            Err(Ok(redis_value!([
                                [0, 1, [name, 6379]],
                                [2, 16383, [name, 6380]]
                            ])))
                        }
                        _ => {
                            assert_eq!(port, 6380);
                            Err(Ok(redis_value!("123")))
                        }
                    }
                }
            },
        );

        // The refresh interval must not apply here. Rate limiting is only earned by
        // having learned the mapping from the redirect itself; when the redirect is
        // unusable, a full refresh is the only way the client can make progress, and
        // throttling it strands the request until retries run out.
        assert_eq!(
            cmd("GET").arg("test").query::<Option<i32>>(&mut connection),
            Ok(Some(123))
        );
        assert_eq!(slot_refreshes.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_cluster_zero_refresh_interval_refreshes_on_every_moved() {
        let name = "zero_refresh_interval";
        let slot_refreshes = Arc::new(atomic::AtomicUsize::new(0));

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .min_slot_refresh_interval(std::time::Duration::ZERO),
            name,
            {
                let slot_refreshes = slot_refreshes.clone();
                move |cmd: &[u8], port| {
                    if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                        slot_refreshes.fetch_add(1, Ordering::SeqCst);
                    }
                    respond_startup_two_nodes(name, cmd)?;
                    match port {
                        6379 => Err(parse_redis_value(
                            format!("-MOVED {TEST_KEY_SLOT} {name}:6380\r\n").as_bytes(),
                        )),
                        6380 => Err(Ok(redis_value!("123"))),
                        _ => panic!("Wrong node"),
                    }
                }
            },
        );

        // The escape hatch back to the historical behaviour has to keep working:
        // the refreshed map re-points the slot at 6379, so each request MOVEDs
        // again and each MOVED refetches the whole map.
        let before = slot_refreshes.load(Ordering::SeqCst);
        for _ in 0..3 {
            assert_eq!(
                cmd("GET").arg("test").query::<Option<i32>>(&mut connection),
                Ok(Some(123))
            );
        }
        assert_eq!(slot_refreshes.load(Ordering::SeqCst) - before, 3);
    }

    #[test]
    fn test_cluster_ask_redirect() {
        let name = "test_cluster_ask_redirect";
        let completed = Arc::new(AtomicI32::new(0));
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]),
            name,
            {
                move |cmd: &[u8], port| {
                    respond_startup_two_nodes(name, cmd)?;
                    // Error twice with io-error, ensure connection is reestablished w/out calling
                    // other node (i.e., not doing a full slot rebuild)
                    let count = completed.fetch_add(1, Ordering::SeqCst);
                    match port {
                        6379 => match count {
                            0 => Err(parse_redis_value(
                                b"-ASK 14000 test_cluster_ask_redirect:6380\r\n",
                            )),
                            _ => panic!("Node should not be called now"),
                        },
                        6380 => match count {
                            1 => {
                                assert!(contains_slice(cmd, b"ASKING"));
                                Err(Ok(Value::Okay))
                            }
                            2 => {
                                assert!(contains_slice(cmd, b"GET"));
                                Err(Ok(redis_value!("123")))
                            }
                            _ => panic!("Node should not be called now"),
                        },
                        _ => panic!("Wrong node"),
                    }
                }
            },
        );

        let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

        assert_eq!(value, Ok(Some(123)));
    }

    #[test]
    fn test_cluster_ask_error_when_new_node_is_added() {
        let name = "ask_with_extra_nodes";

        let requests = atomic::AtomicUsize::new(0);
        let started = atomic::AtomicBool::new(false);

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::new(name, move |cmd: &[u8], port| {
            if !started.load(atomic::Ordering::SeqCst) {
                respond_startup(name, cmd)?;
            }
            started.store(true, atomic::Ordering::SeqCst);

            if is_connection_check(cmd) {
                return Err(Ok(redis_value!(simple:"OK")));
            }

            let i = requests.fetch_add(1, atomic::Ordering::SeqCst);

            match i {
                // Respond that the key exists on a node that does not yet have a connection:
                0 => Err(parse_redis_value(
                    format!("-ASK 123 {name}:6380\r\n").as_bytes(),
                )),
                1 => {
                    assert_eq!(port, 6380);
                    assert!(contains_slice(cmd, b"ASKING"));
                    Err(Ok(Value::Okay))
                }
                2 => {
                    assert_eq!(port, 6380);
                    assert!(contains_slice(cmd, b"GET"));
                    Err(Ok(redis_value!("123")))
                }
                _ => {
                    panic!("Unexpected request: {cmd:?}");
                }
            }
        });

        let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

        assert_eq!(value, Ok(Some(123)));
    }

    #[test]
    fn test_cluster_random_replica_read() {
        let name = "test_cluster_random_replica_read";

        // requests should route to replica
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(RandomReplicaStrategy),
            name,
            move |cmd: &[u8], port| {
                respond_startup_with_replica(name, cmd)?;

                match port {
                    6380 => Err(Ok(redis_value!("123"))),
                    _ => panic!("Wrong node"),
                }
            },
        );

        let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);
        assert_eq!(value, Ok(Some(123)));

        // requests should route to primary
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(RandomReplicaStrategy),
            name,
            move |cmd: &[u8], port| {
                respond_startup_with_replica(name, cmd)?;
                match port {
                    6379 => Err(Ok(redis_value!(simple:"OK"))),
                    _ => panic!("Wrong node"),
                }
            },
        );

        let value = cmd("SET")
            .arg("test")
            .arg("123")
            .query::<Option<Value>>(&mut connection);
        assert_eq!(value, Ok(Some(redis_value!(simple:"OK"))));
    }

    #[test]
    fn test_cluster_uniform_random_connects_to_one_replica_per_shard() {
        let name = "test_cluster_uniform_random_connects_to_one_replica_per_shard";
        let slots_config = vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![6380, 6381],
                slot_range: 0..8192,
            },
            MockSlotRange {
                primary_port: 6382,
                replica_ports: vec![6383, 6384],
                slot_range: 8192..16384,
            },
        ];
        let command_ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let command_ports_clone = Arc::clone(&command_ports);

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .read_routing_strategy(UniformRandom::new()),
            name,
            move |cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, cmd, Some(slots_config.clone()))?;
                if contains_slice(cmd, b"GET") {
                    command_ports_clone.lock().unwrap().push(port);
                    return Err(Ok(Value::Nil));
                }
                Ok(())
            },
        );

        let connected_ports = connection
            .connected_node_addresses()
            .into_iter()
            .map(|addr| addr.port())
            .collect::<Vec<_>>();

        assert!(connected_ports.contains(&6379));
        assert!(connected_ports.contains(&6382));
        assert_eq!(
            connected_ports
                .iter()
                .filter(|port| matches!(**port, 6380 | 6381))
                .count(),
            1
        );
        assert_eq!(
            connected_ports
                .iter()
                .filter(|port| matches!(**port, 6383 | 6384))
                .count(),
            1
        );
        assert_eq!(connected_ports.len(), 4);

        connection
            .route_command(
                cmd("GET").arg("test"),
                RoutingInfo::MultiNode((MultipleNodeRoutingInfo::AllNodes, None)),
            )
            .unwrap();
        command_ports.lock().unwrap().sort_unstable();
        assert_eq!(
            *command_ports.lock().unwrap(),
            vec![6379, 6380, 6381, 6382, 6383, 6384]
        );
        assert_eq!(connection.connected_node_count(), 4);
    }

    #[test]
    fn test_cluster_uniform_random_falls_back_after_selected_replica_io_error() {
        let name = "uniform_random_fallback";
        let slots_config = vec![MockSlotRange {
            primary_port: 6379,
            replica_ports: vec![6380, 6381],
            slot_range: 0..16384,
        }];
        let ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ports_clone = ports.clone();
        let failed_once = Arc::new(atomic::AtomicBool::new(false));
        let failed_once_clone = failed_once.clone();

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(2)
                .read_routing_strategy(UniformRandom::new()),
            name,
            move |cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, cmd, Some(slots_config.clone()))?;

                if contains_slice(cmd, b"GET") {
                    ports_clone.lock().unwrap().push(port);
                    if port != 6379 && !failed_once_clone.swap(true, Ordering::SeqCst) {
                        return Err(Err(RedisError::from(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            "mock selected replica failure",
                        ))));
                    }
                    return Err(Ok(redis_value!("123")));
                }

                Ok(())
            },
        );

        let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

        assert_eq!(value, Ok(Some(123)));
        let recorded = ports.lock().unwrap().clone();
        assert_eq!(recorded.len(), 2);
        assert_ne!(recorded[0], 6379);
        assert_eq!(recorded[1], 6379);
        assert_eq!(connection.connected_node_count(), 2);
    }

    #[test]
    fn test_cluster_uniform_random_falls_back_when_selected_replica_cannot_connect() {
        let name = "uniform_random_connect_fallback";
        let slots_config = vec![MockSlotRange {
            primary_port: 6379,
            replica_ports: vec![6380, 6381],
            slot_range: 0..16384,
        }];
        let read_ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let read_ports_clone = Arc::clone(&read_ports);
        let replica_connect_attempts = Arc::new(atomic::AtomicUsize::new(0));
        let replica_connect_attempts_clone = Arc::clone(&replica_connect_attempts);

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(UniformRandom::new()),
            name,
            move |cmd: &[u8], port| {
                if contains_slice(cmd, b"READONLY") && port != 6379 {
                    replica_connect_attempts_clone.fetch_add(1, Ordering::SeqCst);
                    return Err(Err(RedisError::from(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "mock replica connect failure",
                    ))));
                }
                respond_startup_with_replica_using_config(name, cmd, Some(slots_config.clone()))?;
                if contains_slice(cmd, b"GET") {
                    read_ports_clone.lock().unwrap().push(port);
                    return Err(Ok(redis_value!("123")));
                }
                Ok(())
            },
        );

        let attempts_after_startup = replica_connect_attempts.load(Ordering::SeqCst);
        assert!(attempts_after_startup > 0);

        for _ in 0..2 {
            let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);
            assert_eq!(value, Ok(Some(123)));
        }

        assert_eq!(*read_ports.lock().unwrap(), vec![6379, 6379]);
        assert_eq!(
            replica_connect_attempts.load(Ordering::SeqCst),
            attempts_after_startup
        );
        assert_eq!(connection.connected_node_count(), 1);
    }

    #[test]
    fn test_cluster_uniform_random_replica_required_connects_to_another_replica() {
        let name = "uniform_random_replica_required_connect_fallback";
        let slots_config = vec![MockSlotRange {
            primary_port: 6379,
            replica_ports: vec![6380, 6381],
            slot_range: 0..16384,
        }];
        let failed_replica = Arc::new(atomic::AtomicU16::new(0));
        let failed_replica_clone = Arc::clone(&failed_replica);
        let read_ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let read_ports_clone = Arc::clone(&read_ports);

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(UniformRandom::new()),
            name,
            move |cmd: &[u8], port| {
                if contains_slice(cmd, b"READONLY") && port != 6379 {
                    let failed = failed_replica_clone
                        .compare_exchange(0, port, Ordering::SeqCst, Ordering::SeqCst)
                        .unwrap_or_else(|failed| failed);
                    if failed == port || failed == 0 {
                        return Err(Err(RedisError::from(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            "mock selected replica connect failure",
                        ))));
                    }
                }
                respond_startup_with_replica_using_config(name, cmd, Some(slots_config.clone()))?;
                if contains_slice(cmd, b"GET") {
                    read_ports_clone.lock().unwrap().push(port);
                    return Err(Ok(redis_value!("123")));
                }
                Ok(())
            },
        );

        let failed_replica = failed_replica.load(Ordering::SeqCst);
        assert!(matches!(failed_replica, 6380 | 6381));
        let mut command = cmd("GET");
        command.arg("test");
        let value = connection
            .route_command(
                &command,
                RoutingInfo::SingleNode(SingleNodeRoutingInfo::SpecificNode(Route::with_key(
                    "test",
                    SlotAddr::ReplicaRequired,
                ))),
            )
            .unwrap();

        assert_eq!(value, redis_value!("123"));
        let read_ports = read_ports.lock().unwrap();
        assert_eq!(read_ports.len(), 1);
        assert_ne!(read_ports[0], 6379);
        assert_ne!(read_ports[0], failed_replica);
        assert_eq!(connection.connected_node_count(), 2);
    }

    #[test]
    fn test_cluster_connected_node_addresses_respect_replica_filter() {
        let name = "test_cluster_connected_node_addresses_respect_replica_filter";
        let slots_config = vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![6380, 6381],
                slot_range: 0..8192,
            },
            MockSlotRange {
                primary_port: 6382,
                replica_ports: vec![6383, 6384],
                slot_range: 8192..16384,
            },
        ];

        let MockEnv { connection, .. } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .replica_filter(|addr| matches!(addr.port(), 6380 | 6384)),
            name,
            move |cmd: &[u8], _port| {
                respond_startup_with_replica_using_config(name, cmd, Some(slots_config.clone()))
            },
        );

        let ports = connection
            .connected_node_addresses()
            .into_iter()
            .map(|addr| addr.port())
            .collect::<Vec<_>>();
        assert_eq!(ports, vec![6379, 6380, 6382, 6384]);
        assert_eq!(connection.connected_node_count(), 4);
    }

    #[test]
    fn test_cluster_replica_filter_drop_all_connects_primaries_only() {
        let name = "test_cluster_replica_filter_drop_all_connects_primaries_only";
        let slots_config = vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![6380, 6381],
                slot_range: 0..8192,
            },
            MockSlotRange {
                primary_port: 6382,
                replica_ports: vec![6383, 6384],
                slot_range: 8192..16384,
            },
        ];

        let MockEnv { connection, .. } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).replica_filter(|_addr| false),
            name,
            move |cmd: &[u8], _port| {
                respond_startup_with_replica_using_config(name, cmd, Some(slots_config.clone()))
            },
        );

        let ports = connection
            .connected_node_addresses()
            .into_iter()
            .map(|addr| addr.port())
            .collect::<Vec<_>>();
        assert_eq!(ports, vec![6379, 6382]);
        assert_eq!(connection.connected_node_count(), 2);
    }

    fn bulk(value: &str) -> Value {
        Value::BulkString(value.as_bytes().to_vec())
    }

    fn cluster_shards_with_zones(
        name: &str,
        slots_config: &[MockSlotRange],
        zones: &[(u16, &str)],
    ) -> Value {
        let zone_for_port = |port| {
            zones
                .iter()
                .find_map(|(candidate_port, zone)| (*candidate_port == port).then_some(*zone))
                .unwrap_or("unknown-zone")
        };

        Value::Array(
            slots_config
                .iter()
                .map(|slot| {
                    let mut nodes = vec![Value::Map(vec![
                        (bulk("endpoint"), bulk(name)),
                        (bulk("port"), Value::Int(slot.primary_port as i64)),
                        (
                            bulk("availability-zone"),
                            bulk(zone_for_port(slot.primary_port)),
                        ),
                    ])];
                    nodes.extend(slot.replica_ports.iter().map(|port| {
                        Value::Map(vec![
                            (bulk("endpoint"), bulk(name)),
                            (bulk("port"), Value::Int(*port as i64)),
                            (bulk("availability-zone"), bulk(zone_for_port(*port))),
                        ])
                    }));

                    Value::Map(vec![
                        (
                            bulk("slots"),
                            Value::Array(vec![
                                Value::Int(slot.slot_range.start as i64),
                                Value::Int(slot.slot_range.end as i64),
                            ]),
                        ),
                        (bulk("nodes"), Value::Array(nodes)),
                    ])
                })
                .collect(),
        )
    }

    #[test]
    fn test_cluster_zonal_read_routes_locally_and_prunes_remote_replica_connections() {
        let name = "test_cluster_zonal_read_routes_locally_and_prunes_remote_replica_connections";
        let ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ports_clone = ports.clone();
        let slots_config = vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![6380, 6381],
                slot_range: 0..8192,
            },
            MockSlotRange {
                primary_port: 6382,
                replica_ports: vec![6383, 6384],
                slot_range: 8192..16384,
            },
        ];
        let zones = vec![
            (6379, "us-east-1b"),
            (6380, "us-east-1b"),
            (6381, "us-east-1c"),
            (6382, "us-east-1c"),
            (6383, "us-east-1c"),
            (6384, "us-east-1d"),
        ];

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_from_zonal_replicas("us-east-1b"),
            name,
            move |cmd: &[u8], port| {
                if is_connection_check(cmd) {
                    return Err(Ok(redis_value!(simple:"OK")));
                }
                if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                    return respond_startup_with_replica_using_config(
                        name,
                        cmd,
                        Some(slots_config.clone()),
                    );
                }
                if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SHARDS") {
                    return Err(Ok(cluster_shards_with_zones(name, &slots_config, &zones)));
                }
                if contains_slice(cmd, b"GET") {
                    ports_clone.lock().unwrap().push(port);
                    return Err(Ok(redis_value!("123")));
                }
                Ok(())
            },
        );

        let connected_ports = connection
            .connected_node_addresses()
            .into_iter()
            .map(|addr| addr.port())
            .collect::<Vec<_>>();
        assert_eq!(connected_ports, vec![6379, 6380, 6382, 6383, 6384]);

        let _: Option<i32> = cmd("GET").arg("test").query(&mut connection).unwrap();
        let _: Option<i32> = cmd("GET").arg("{foo}test").query(&mut connection).unwrap();
        let _: Option<i32> = cmd("GET").arg("{foo}test").query(&mut connection).unwrap();

        assert_eq!(ports.lock().unwrap().clone(), vec![6380, 6383, 6384]);
    }

    #[test]
    fn test_cluster_zonal_read_reuses_info_server_discovery_results_after_refresh() {
        let name = "test_cluster_zonal_read_reuses_info_server_discovery_results_after_refresh";
        let shards_calls = Arc::new(atomic::AtomicUsize::new(0));
        let shards_calls_clone = shards_calls.clone();
        let info_calls = Arc::new(atomic::AtomicUsize::new(0));
        let info_calls_clone = info_calls.clone();
        let get_calls = Arc::new(atomic::AtomicUsize::new(0));
        let get_calls_clone = get_calls.clone();
        let slots_config = vec![MockSlotRange {
            primary_port: 6379,
            replica_ports: vec![6380, 6381],
            slot_range: 0..16384,
        }];

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .read_from_zonal_replicas("us-east-1b"),
            name,
            move |cmd: &[u8], port| {
                if is_connection_check(cmd) {
                    return Err(Ok(redis_value!(simple:"OK")));
                }
                if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                    return respond_startup_with_replica_using_config(
                        name,
                        cmd,
                        Some(slots_config.clone()),
                    );
                }
                if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SHARDS") {
                    shards_calls_clone.fetch_add(1, atomic::Ordering::SeqCst);
                    return Err(parse_redis_value(b"-ERR unknown command\r\n"));
                }
                if contains_slice(cmd, b"INFO") && contains_slice(cmd, b"SERVER") {
                    info_calls_clone.fetch_add(1, atomic::Ordering::SeqCst);
                    let zone = if port == 6380 {
                        "us-east-1b"
                    } else {
                        "us-east-1c"
                    };
                    return Err(Ok(Value::BulkString(
                        format!("# Server\r\navailability_zone:{zone}\r\n").into_bytes(),
                    )));
                }
                if contains_slice(cmd, b"GET") {
                    if get_calls_clone.fetch_add(1, atomic::Ordering::SeqCst) == 0 {
                        return Err(parse_redis_value(b"-MOVED 123\r\n"));
                    }
                    assert_eq!(port, 6380);
                    return Err(Ok(redis_value!("123")));
                }
                Ok(())
            },
        );

        let shards_calls_after_setup = shards_calls.load(atomic::Ordering::SeqCst);
        let info_calls_after_setup = info_calls.load(atomic::Ordering::SeqCst);
        assert!(shards_calls_after_setup >= 1);
        assert!(info_calls_after_setup >= 3);
        assert_eq!(
            connection
                .connected_node_addresses()
                .into_iter()
                .map(|addr| addr.port())
                .collect::<Vec<_>>(),
            vec![6379, 6380]
        );

        let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

        assert_eq!(value, Ok(Some(123)));
        assert_eq!(
            shards_calls.load(atomic::Ordering::SeqCst),
            shards_calls_after_setup
        );
        assert_eq!(
            info_calls.load(atomic::Ordering::SeqCst),
            info_calls_after_setup
        );
    }

    #[test]
    fn test_cluster_zonal_read_shared_strategy_reuses_discovered_az_metadata() {
        let name = "test_cluster_zonal_read_shared_strategy_reuses_discovered_az_metadata";
        let shards_calls = Arc::new(atomic::AtomicUsize::new(0));
        let slots_config = vec![MockSlotRange {
            primary_port: 6379,
            replica_ports: vec![6380, 6381],
            slot_range: 0..16384,
        }];
        let zones = vec![
            (6379, "us-east-1b"),
            (6380, "us-east-1b"),
            (6381, "us-east-1c"),
        ];
        let strategy = ZonalReadRoutingStrategy::shared("us-east-1b");
        let make_handler = {
            let slots_config = slots_config.clone();
            let zones = zones.clone();
            move |shards_calls: Arc<atomic::AtomicUsize>| {
                let slots_config = slots_config.clone();
                let zones = zones.clone();
                move |cmd: &[u8], _port| {
                    if is_connection_check(cmd) {
                        return Err(Ok(redis_value!(simple:"OK")));
                    }
                    if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SLOTS") {
                        return respond_startup_with_replica_using_config(
                            name,
                            cmd,
                            Some(slots_config.clone()),
                        );
                    }
                    if contains_slice(cmd, b"CLUSTER") && contains_slice(cmd, b"SHARDS") {
                        shards_calls.fetch_add(1, Ordering::SeqCst);
                        return Err(Ok(cluster_shards_with_zones(name, &slots_config, &zones)));
                    }
                    Ok(())
                }
            }
        };

        let _first = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .read_routing_strategy(strategy.clone()),
            name,
            make_handler(shards_calls.clone()),
        );
        assert_eq!(shards_calls.load(Ordering::SeqCst), 1);

        let MockEnv { connection, .. } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .read_routing_strategy(strategy.clone()),
            name,
            make_handler(shards_calls.clone()),
        );

        assert_eq!(shards_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            connection
                .connected_node_addresses()
                .into_iter()
                .map(|addr| addr.port())
                .collect::<Vec<_>>(),
            vec![6379, 6380]
        );
    }

    #[test]
    fn test_cluster_client_name_factory_names_each_connected_node() {
        let name = "test_cluster_client_name_factory_names_each_connected_node";
        let slots_config = vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![6380],
                slot_range: 0..8192,
            },
            MockSlotRange {
                primary_port: 6381,
                replica_ports: vec![6382],
                slot_range: 8192..16384,
            },
        ];
        let seen = Arc::new(std::sync::Mutex::new(Vec::<(u16, String)>::new()));
        let seen_for_handler = seen.clone();

        let _env = MockEnv::with_client_builder_and_config(
            ClusterClient::builder(vec![&*format!("redis://{name}")]),
            redis::cluster::ClusterConfig::new()
                .set_client_name_factory(|addr| format!("mock-client-{}", addr.port())),
            name,
            move |cmd: &[u8], port| {
                if contains_slice(cmd, b"CLIENT") && contains_slice(cmd, b"SETNAME") {
                    seen_for_handler
                        .lock()
                        .unwrap()
                        .push((port, String::from_utf8_lossy(cmd).into_owned()));
                    return Err(Ok(redis_value!(simple:"OK")));
                }
                respond_startup_with_replica_using_config(name, cmd, Some(slots_config.clone()))
            },
        );

        let mut seen = seen.lock().unwrap().clone();
        seen.sort_by_key(|(port, _)| *port);
        assert_eq!(
            seen.iter().map(|(port, _)| *port).collect::<Vec<_>>(),
            vec![6379, 6380, 6381, 6382]
        );
        for (port, cmd) in seen {
            assert!(cmd.contains(&format!("mock-client-{port}")));
        }
    }

    #[test]
    fn test_cluster_client_name_factory_names_new_nodes_after_refresh() {
        let name = "test_cluster_client_name_factory_names_new_nodes_after_refresh";
        let requests = atomic::AtomicUsize::new(0);
        let started = atomic::AtomicBool::new(false);
        let seen = Arc::new(std::sync::Mutex::new(Vec::<(u16, String)>::new()));
        let seen_for_handler = seen.clone();

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder_and_config(
            ClusterClient::builder(vec![&*format!("redis://{name}")]),
            redis::cluster::ClusterConfig::new()
                .set_client_name_factory(|addr| format!("mock-client-{}", addr.port())),
            name,
            move |cmd: &[u8], port| {
                if contains_slice(cmd, b"CLIENT") && contains_slice(cmd, b"SETNAME") {
                    seen_for_handler
                        .lock()
                        .unwrap()
                        .push((port, String::from_utf8_lossy(cmd).into_owned()));
                    return Err(Ok(redis_value!(simple:"OK")));
                }

                if !started.load(atomic::Ordering::SeqCst) {
                    respond_startup(name, cmd)?;
                }
                started.store(true, atomic::Ordering::SeqCst);

                if is_connection_check(cmd) {
                    return Err(Ok(redis_value!(simple:"OK")));
                }

                let i = requests.fetch_add(1, atomic::Ordering::SeqCst);
                match i {
                    0 => Err(parse_redis_value(b"-MOVED 123\r\n")),
                    1 => Err(Ok(redis_value!([
                        [0, 1, [name, 6379]],
                        [2, 16383, [name, 6380]]
                    ]))),
                    _ => {
                        assert_eq!(port, 6380);
                        Err(Ok(redis_value!("123")))
                    }
                }
            },
        );

        let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);
        assert_eq!(value, Ok(Some(123)));

        let mut seen = seen.lock().unwrap().clone();
        seen.sort_by_key(|(port, _)| *port);
        assert_eq!(
            seen.iter().map(|(port, _)| *port).collect::<Vec<_>>(),
            vec![6379, 6380]
        );
        for (port, cmd) in seen {
            assert!(cmd.contains(&format!("mock-client-{port}")));
        }
    }

    #[test]
    fn test_cluster_round_robin_read() {
        let name = "test_cluster_round_robin_read";
        let ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ports_clone = ports.clone();

        // Two shards, each with two replicas.
        // Shard 1 (slots 0..8192):    primary 6379, replicas 6380, 6381
        // Shard 2 (slots 8192..16384): primary 6382, replicas 6383, 6384
        let slots_config = vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![6380, 6381],
                slot_range: 0..8192,
            },
            MockSlotRange {
                primary_port: 6382,
                replica_ports: vec![6383, 6384],
                slot_range: 8192..16384,
            },
        ];

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(RoundRobinReplicaStrategy::new()),
            name,
            move |cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, cmd, Some(slots_config.clone()))?;
                if contains_slice(cmd, b"GET") {
                    ports_clone.lock().unwrap().push(port);
                    return Err(Ok(redis_value!("123")));
                }
                Ok(())
            },
        );

        // "test" hashes to slot 6918 → shard 1 (replicas 6380, 6381).
        // "{foo}test" hashes to slot 12182 → shard 2 (replicas 6383, 6384).
        // Interleave reads across both shards and verify each shard
        // round-robins independently.
        for key in [
            "test",
            "{foo}test",
            "test",
            "{foo}test",
            "test",
            "{foo}test",
        ] {
            let _: Option<i32> = cmd("GET").arg(key).query(&mut connection).unwrap();
        }

        let recorded = ports.lock().unwrap().clone();
        assert_eq!(recorded, vec![6380, 6383, 6381, 6384, 6380, 6383]);
    }

    #[test]
    fn test_cluster_round_robin_multi_shard_read_advances_once_per_shard() {
        let name = "test_cluster_round_robin_multi_shard_read_advances_once_per_shard";
        let slots_config = vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![6380, 6381],
                slot_range: 0..8192,
            },
            MockSlotRange {
                primary_port: 6382,
                replica_ports: vec![6383, 6384],
                slot_range: 8192..16384,
            },
        ];
        let mut command = cmd("MGET");
        command.arg("test").arg("{foo}test");
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(RoundRobinReplicaStrategy::new()),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(
                    name,
                    received_cmd,
                    Some(slots_config.clone()),
                )?;
                if contains_slice(received_cmd, b"MGET") {
                    return Err(Ok(Value::Array(vec![redis_value!(port.to_string())])));
                }
                Ok(())
            },
        );

        let first = command.query::<Vec<String>>(&mut connection).unwrap();
        let second = command.query::<Vec<String>>(&mut connection).unwrap();

        assert_eq!(first, vec!["6380", "6383"]);
        assert_eq!(second, vec!["6381", "6384"]);
    }

    #[test]
    fn test_cluster_io_error() {
        let name = "test_cluster_io_error";
        let completed = Arc::new(AtomicI32::new(0));
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")]).retries(2),
            name,
            move |cmd: &[u8], port| {
                respond_startup_two_nodes(name, cmd)?;
                // Error twice with io-error, ensure connection is reestablished w/out calling
                // other node (i.e., not doing a full slot rebuild)
                match port {
                    6380 => panic!("Node should not be called"),
                    _ => match completed.fetch_add(1, Ordering::SeqCst) {
                        0..=1 => Err(Err(RedisError::from(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            "mock-io-error",
                        )))),
                        _ => Err(Ok(redis_value!("123"))),
                    },
                }
            },
        );

        let value = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

        assert_eq!(value, Ok(Some(123)));
    }

    #[test]
    fn test_cluster_non_retryable_error_should_not_retry() {
        let name = "test_cluster_non_retryable_error_should_not_retry";
        let completed = Arc::new(AtomicI32::new(0));
        let MockEnv { mut connection, .. } = MockEnv::new(name, {
            let completed = completed.clone();
            move |cmd: &[u8], _| {
                respond_startup_two_nodes(name, cmd)?;
                // Error twice with io-error, ensure connection is reestablished w/out calling
                // other node (i.e., not doing a full slot rebuild)
                completed.fetch_add(1, Ordering::SeqCst);
                Err(Err((ServerErrorKind::ReadOnly.into(), "").into()))
            }
        });

        let result = cmd("GET").arg("test").query::<Option<i32>>(&mut connection);

        assert!(
            matches!(&result, Err(err) if err.kind() == ServerErrorKind::ReadOnly.into()),
            "{result:?}",
        );
        assert_eq!(completed.load(Ordering::SeqCst), 1);
    }

    fn test_cluster_fan_out(
        name: &'static str,
        command: &'static str,
        expected_ports: Vec<u16>,
        slots_config: Option<Vec<MockSlotRange>>,
    ) {
        let found_ports = Arc::new(std::sync::Mutex::new(Vec::new()));
        let ports_clone = found_ports.clone();
        let mut cmd = redis::Cmd::new();
        for arg in command.split_whitespace() {
            cmd.arg(arg);
        }
        let packed_cmd = cmd.get_packed_command();
        // requests should route to replica
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(RandomReplicaStrategy),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(
                    name,
                    received_cmd,
                    slots_config.clone(),
                )?;
                if received_cmd == packed_cmd {
                    ports_clone.lock().unwrap().push(port);
                    return Err(Ok(redis_value!(simple:"OK")));
                }
                Ok(())
            },
        );

        let _ = cmd.query::<Option<()>>(&mut connection);
        found_ports.lock().unwrap().sort();
        // MockEnv creates 2 mock connections.
        assert_eq!(*found_ports.lock().unwrap(), expected_ports);
    }

    #[test]
    fn test_cluster_fan_out_to_all_primaries() {
        test_cluster_fan_out(
            "test_cluster_fan_out_to_all_primaries",
            "FLUSHALL",
            vec![6379, 6381],
            None,
        );
    }

    #[test]
    fn test_cluster_fan_out_to_all_nodes() {
        test_cluster_fan_out(
            "test_cluster_fan_out_to_all_nodes",
            "CONFIG SET",
            vec![6379, 6380, 6381, 6382],
            None,
        );
    }

    #[test]
    fn test_cluster_fan_out_out_once_to_each_primary_when_no_replicas_are_available() {
        test_cluster_fan_out(
            "test_cluster_fan_out_out_once_to_each_primary_when_no_replicas_are_available",
            "CONFIG SET",
            vec![6379, 6381],
            Some(vec![
                MockSlotRange {
                    primary_port: 6379,
                    replica_ports: Vec::new(),
                    slot_range: (0..8191),
                },
                MockSlotRange {
                    primary_port: 6381,
                    replica_ports: Vec::new(),
                    slot_range: (8192..16383),
                },
            ]),
        );
    }

    #[test]
    fn test_cluster_fan_out_out_once_even_if_primary_has_multiple_slot_ranges() {
        test_cluster_fan_out(
            "test_cluster_fan_out_out_once_even_if_primary_has_multiple_slot_ranges",
            "CONFIG SET",
            vec![6379, 6380, 6381, 6382],
            Some(vec![
                MockSlotRange {
                    primary_port: 6379,
                    replica_ports: vec![6380],
                    slot_range: (0..4000),
                },
                MockSlotRange {
                    primary_port: 6381,
                    replica_ports: vec![6382],
                    slot_range: (4001..8191),
                },
                MockSlotRange {
                    primary_port: 6379,
                    replica_ports: vec![6380],
                    slot_range: (8192..8200),
                },
                MockSlotRange {
                    primary_port: 6381,
                    replica_ports: vec![6382],
                    slot_range: (8201..16383),
                },
            ]),
        );
    }

    #[test]
    fn test_cluster_split_multi_shard_command_and_combine_arrays_of_values() {
        let name = "test_cluster_split_multi_shard_command_and_combine_arrays_of_values";
        let mut cmd = cmd("MGET");
        cmd.arg("foo").arg("bar").arg("baz");
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(RandomReplicaStrategy),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                let cmd_str = std::str::from_utf8(received_cmd).unwrap();
                let results = ["foo", "bar", "baz"]
                    .iter()
                    .filter_map(|expected_key| {
                        if cmd_str.contains(expected_key) {
                            Some(redis_value!(format!("{expected_key}-{port}")))
                        } else {
                            None
                        }
                    })
                    .collect();
                Err(Ok(Value::Array(results)))
            },
        );

        let result = cmd.query::<Vec<String>>(&mut connection).unwrap();
        assert_eq!(result, vec!["foo-6382", "bar-6380", "baz-6380"]);
    }

    #[test]
    fn test_cluster_uniform_random_multi_shard_read_falls_back_to_primaries() {
        let name = "uniform_random_multi_shard_fallback";
        let replica_connect_attempts = Arc::new(atomic::AtomicUsize::new(0));
        let replica_connect_attempts_clone = Arc::clone(&replica_connect_attempts);
        let mut command = cmd("MGET");
        command.arg("foo").arg("bar").arg("baz");
        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(UniformRandom::new()),
            name,
            move |received_cmd: &[u8], port| {
                if contains_slice(received_cmd, b"READONLY") && matches!(port, 6380 | 6382) {
                    replica_connect_attempts_clone.fetch_add(1, Ordering::SeqCst);
                    return Err(Err(RedisError::from(std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        "mock replica connect failure",
                    ))));
                }
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                let cmd_str = std::str::from_utf8(received_cmd).unwrap();
                let results = ["foo", "bar", "baz"]
                    .iter()
                    .filter(|expected_key| cmd_str.contains(*expected_key))
                    .map(|expected_key| redis_value!(format!("{expected_key}-{port}")))
                    .collect();
                Err(Ok(Value::Array(results)))
            },
        );

        let attempts_after_startup = replica_connect_attempts.load(Ordering::SeqCst);
        assert!(attempts_after_startup >= 2);
        for _ in 0..2 {
            let result = command.query::<Vec<String>>(&mut connection).unwrap();
            assert_eq!(result, vec!["foo-6381", "bar-6379", "baz-6379"]);
        }
        assert_eq!(
            replica_connect_attempts.load(Ordering::SeqCst),
            attempts_after_startup
        );
    }

    #[test]
    fn test_cluster_route_correctly_on_packed_transaction_with_single_node_requests() {
        let name = "test_cluster_route_correctly_on_packed_transaction_with_single_node_requests";
        let mut pipeline = redis::pipe();
        pipeline.atomic().set("foo", "bar").get("foo");
        let packed_pipeline = pipeline.get_packed_pipeline();

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(RandomReplicaStrategy),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                if port == 6381 {
                    return Err(Ok(redis_value!(["OK", "QUEUED", "QUEUED", ["OK", "bar"]])));
                }
                Err(Err(RedisError::from(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    format!("wrong port: {port}"),
                ))))
            },
        );

        let result = connection
            .req_packed_commands(&packed_pipeline, 3, 1)
            .unwrap();
        assert_eq!(result, vec![redis_value!("OK"), redis_value!("bar"),]);
    }

    #[test]
    fn test_cluster_route_correctly_on_packed_transaction_with_single_node_requests2() {
        let name = "test_cluster_route_correctly_on_packed_transaction_with_single_node_requests2";
        let mut pipeline = redis::pipe();
        pipeline.atomic().set("foo", "bar").get("foo");
        let packed_pipeline = pipeline.get_packed_pipeline();
        let expected_result = redis_value!(["OK", "QUEUED", "QUEUED", ["OK", "bar"]]);
        let cloned_result = expected_result.clone();

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(RandomReplicaStrategy),
            name,
            move |received_cmd: &[u8], port| {
                respond_startup_with_replica_using_config(name, received_cmd, None)?;
                if port == 6381 {
                    return Err(Ok(cloned_result.clone()));
                }
                Err(Err(RedisError::from(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    format!("wrong port: {port}"),
                ))))
            },
        );

        let result = connection.req_packed_command(&packed_pipeline).unwrap();
        assert_eq!(result, expected_result);
    }

    #[test]
    fn test_cluster_can_be_created_with_partial_slot_coverage() {
        let name = "test_cluster_can_be_created_with_partial_slot_coverage";
        let slots_config = Some(vec![
            MockSlotRange {
                primary_port: 6379,
                replica_ports: vec![],
                slot_range: (0..8000),
            },
            MockSlotRange {
                primary_port: 6381,
                replica_ports: vec![],
                slot_range: (8201..16380),
            },
        ]);

        let MockEnv {
            mut connection,
            handler: _handler,
            ..
        } = MockEnv::with_client_builder(
            ClusterClient::builder(vec![&*format!("redis://{name}")])
                .retries(0)
                .read_routing_strategy(RandomReplicaStrategy),
            name,
            move |received_cmd: &[u8], _| {
                respond_startup_with_replica_using_config(
                    name,
                    received_cmd,
                    slots_config.clone(),
                )?;
                Err(Ok(Value::SimpleString("PONG".into())))
            },
        );

        let res = connection.req_command(&redis::cmd("PING"));
        assert_matches!(res, Ok(_));
    }

    #[test]
    fn test_cluster_handle_complete_server_disconnect_without_panicking() {
        let cluster =
            TestClusterContext::new_with_cluster_client_builder(|builder| builder.retries(2));

        let mut connection = cluster.connection();
        drop(cluster);
        for _ in 0..5 {
            let cmd = cmd("PING");
            let result = connection
                .route_command(&cmd, RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random));
            // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
            assert_matches!(result, Err(_));
            // This will route to all nodes - different path through the code.
            let result = connection.req_packed_command(&cmd.get_packed_command());
            // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
            assert_matches!(result, Err(_));
        }
    }

    #[test]
    fn test_cluster_reconnect_after_complete_server_disconnect() {
        let cluster = TestClusterContext::new_insecure_with_cluster_client_builder(|builder| {
            builder.retries(3)
        });

        let ports: Vec<_> = cluster.get_ports();

        let mut connection = cluster.connection();
        drop(cluster);

        let cmd = cmd("PING");

        let result =
            connection.route_command(&cmd, RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random));
        // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
        assert_matches!(result, Err(_));

        // This will route to all nodes - different path through the code.
        let result = connection.req_packed_command(&cmd.get_packed_command());
        // TODO - this should be a NoConnectionError, but ATM we get the errors from the failing
        assert_matches!(result, Err(_));

        let _cluster = RedisCluster::new(RedisClusterConfiguration {
            ports,
            ..Default::default()
        });

        let result = connection
            .route_command(&cmd, RoutingInfo::SingleNode(SingleNodeRoutingInfo::Random))
            .unwrap();
        assert_eq!(result, redis_value!(simple:"PONG"));
    }

    #[test]
    fn test_cluster_reconnect_after_complete_server_disconnect_route_to_many() {
        let cluster = TestClusterContext::new_insecure_with_cluster_client_builder(|builder| {
            builder.retries(3)
        });

        let ports: Vec<_> = cluster.get_ports();

        let mut connection = cluster.connection();
        drop(cluster);

        // recreate cluster
        let _cluster: RedisCluster = RedisCluster::new(RedisClusterConfiguration {
            ports,
            ..Default::default()
        });

        let cmd = cmd("PING");
        // explicitly route to all primaries and request all succeeded
        let result = connection
            .route_command(
                &cmd,
                RoutingInfo::MultiNode((
                    MultipleNodeRoutingInfo::AllMasters,
                    Some(redis::cluster_routing::ResponsePolicy::AllSucceeded),
                )),
            )
            .unwrap();
        assert_eq!(result, redis_value!(simple:"PONG"));
    }

    #[test]
    fn fail_on_empty_command() {
        let ctx = TestClusterContext::new();
        let mut connection = ctx.connection();

        let error: RedisError = cluster_pipe().query::<String>(&mut connection).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Client);
        assert_eq!(error.to_string(), "empty command - Client");

        let error: RedisError = redis::Cmd::new()
            .query::<String>(&mut connection)
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Client);
        assert_eq!(error.to_string(), "empty command - Client");
    }

    #[cfg(feature = "tls-rustls")]
    mod mtls_test {
        use super::*;
        use crate::support::mtls_test::create_cluster_client_from_cluster;

        #[test]
        fn test_cluster_basics_with_mtls() {
            let cluster = TestClusterContext::new_with_mtls();

            let client = create_cluster_client_from_cluster(&cluster, true).unwrap();
            let mut con = client.get_connection().unwrap();

            redis::cmd("SET")
                .arg("{x}key1")
                .arg(b"foo")
                .exec(&mut con)
                .unwrap();
            redis::cmd("SET")
                .arg(&["{x}key2", "bar"])
                .exec(&mut con)
                .unwrap();

            assert_eq!(
                redis::cmd("MGET")
                    .arg(&["{x}key1", "{x}key2"])
                    .query(&mut con),
                Ok(("foo".to_string(), b"bar".to_vec()))
            );
        }

        #[test]
        fn test_cluster_should_not_connect_without_mtls() {
            let cluster = TestClusterContext::new_with_mtls();

            let client = create_cluster_client_from_cluster(&cluster, false).unwrap();
            let connection = client.get_connection();

            match cluster
                .cluster
                .servers
                .first()
                .unwrap()
                .connection_info()
                .addr()
            {
                redis::ConnectionAddr::TcpTls { .. } => {
                    if connection.is_ok() {
                        panic!(
                            "Must NOT be able to connect without client credentials if server accepts TLS"
                        );
                    }
                }
                _ => {
                    if let Err(e) = connection {
                        panic!(
                            "Must be able to connect without client credentials if server does NOT accept TLS: {e:?}"
                        );
                    }
                }
            }
        }
    }
}
