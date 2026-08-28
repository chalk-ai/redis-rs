#![cfg(feature = "cluster")]

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{BatchSize, Criterion, Throughput, criterion_group, criterion_main};
use redis::cluster::cluster_pipe;
use redis::parse_redis_value;
use redis_test::redis_value;

use support::{MockEnv, respond_startup};

#[allow(dead_code)]
#[path = "../tests/support/mock_cluster.rs"]
mod support;

const COMMANDS: usize = 1_500;
const REPEATED_SLOT: u16 = 16_287;
const NEW_PRIMARIES: usize = 24;
static ENV_ID: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Copy)]
enum HintShape {
    Repeated,
    Distributed,
}

impl HintShape {
    fn name(self) -> &'static str {
        match self {
            Self::Repeated => "repeated_slot",
            Self::Distributed => "distinct_slots_24_primaries",
        }
    }

    fn hint(self, response: usize) -> (u16, u16) {
        match self {
            Self::Repeated => (REPEATED_SLOT, 6380),
            // Multiplication by an odd number permutes the 16384-slot domain,
            // giving a deterministic shuffled order without setup in the timed
            // region. Twenty-four destinations model the report's 12→36 reshard.
            Self::Distributed => (
                ((response * 8191 + 17) % 16384) as u16,
                6380 + (response % NEW_PRIMARIES) as u16,
            ),
        }
    }
}

fn make_case(shape: HintShape) -> (MockEnv, redis::cluster::ClusterPipeline) {
    let id = format!(
        "bench_cluster_moved_pipeline_{}_{}",
        shape.name(),
        ENV_ID.fetch_add(1, Ordering::Relaxed)
    );
    let response = AtomicUsize::new(0);
    let handler_id = id.clone();
    let env = MockEnv::new(&id, move |cmd: &[u8], port| {
        respond_startup(&handler_id, cmd)?;

        match port {
            6379 => {
                let (slot, redirect_port) = shape.hint(response.fetch_add(1, Ordering::Relaxed));
                Err(parse_redis_value(
                    format!("-MOVED {slot} {handler_id}:{redirect_port}\r\n").as_bytes(),
                ))
            }
            6380..=6403 => Err(Ok(redis_value!("value"))),
            _ => panic!("unexpected mock node: {port}"),
        }
    });

    let mut pipeline = cluster_pipe();
    for command in 0..COMMANDS {
        pipeline.cmd("GET").arg(format!("{{x}}key{command}"));
    }
    (env, pipeline)
}

fn run_case(shape: HintShape) -> Vec<String> {
    let (mut env, pipeline) = make_case(shape);
    pipeline.query(&mut env.connection).unwrap()
}

fn bench_cluster_moved_pipeline(c: &mut Criterion) {
    let mut group = c.benchmark_group("cluster_moved_pipeline");
    group.throughput(Throughput::Elements(COMMANDS as u64));
    group.sample_size(20);
    group.warm_up_time(Duration::from_secs(1));
    group.measurement_time(Duration::from_secs(3));

    for shape in [HintShape::Repeated, HintShape::Distributed] {
        let expected = run_case(shape);
        assert_eq!(expected, vec!["value"; COMMANDS]);

        group.bench_function(shape.name(), |b| {
            b.iter_batched(
                || make_case(shape),
                |(mut env, pipeline)| {
                    let values = pipeline.query::<Vec<String>>(&mut env.connection).unwrap();
                    black_box(values);
                },
                BatchSize::PerIteration,
            );
        });
    }

    group.finish();
}

criterion_group!(moved_pipeline_benches, bench_cluster_moved_pipeline);
criterion_main!(moved_pipeline_benches);
