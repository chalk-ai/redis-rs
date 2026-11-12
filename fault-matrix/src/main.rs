//! Load harness for the cluster fault regression matrix.
//!
//! Builds one zonal `ClusterClient` per availability zone, the way a service
//! process does, and drives it from one thread per sync connection: all but the
//! last connection of each zone read random preloaded keys, the last one writes.
//! It records, per zone and per 250 ms bucket, how many commands finished within
//! the caller deadline, finished late, errored, or (for reads) found nothing for
//! a key that must exist, and samples how many threads are stuck past the
//! deadline. `run.py` drives it; see README.md.

use std::io::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rand::Rng;
use serde_json::json;

const ZONES: [&str; 3] = ["us-east-1a", "us-east-1b", "us-east-1c"];
const BUCKET_MS: u64 = 250;
const VALUE: &[u8] = b"vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv";

/// Outcome of one command, as counted by the harness.
#[derive(Clone, Copy)]
enum Kind {
    /// Finished within the deadline with an answer.
    Ok = 0,
    /// Finished with an answer, after the deadline.
    Late = 1,
    /// Returned an error.
    Err = 2,
    /// A read that found nothing for a key that was preloaded and never deleted.
    Missing = 3,
}
const KINDS: usize = 4;

/// Whether a thread reads or writes.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Read = 0,
    Write = 1,
}

/// Which preloaded key set a command used: keys that lived on shard A at boot,
/// or every other key.
const KEY_SETS: usize = 2;

/// Outcome counters shared by every thread, so a thread still blocked when the
/// run ends keeps everything it completed.
struct Counts {
    buckets: usize,
    cells: Vec<AtomicU32>,
}

impl Counts {
    fn new(buckets: usize) -> Self {
        let len = 2 * ZONES.len() * KEY_SETS * buckets * KINDS;
        Self {
            buckets,
            cells: (0..len).map(|_| AtomicU32::new(0)).collect(),
        }
    }

    fn idx(&self, role: Role, zone: usize, keys: usize, bucket: usize, kind: Kind) -> usize {
        ((((role as usize) * ZONES.len() + zone) * KEY_SETS + keys) * self.buckets + bucket) * KINDS
            + kind as usize
    }

    fn add(&self, role: Role, zone: usize, keys: usize, bucket: usize, kind: Kind) {
        self.cells[self.idx(role, zone, keys, bucket, kind)].fetch_add(1, Ordering::Relaxed);
    }

    fn series(&self, role: Role, zone: usize, keys: usize, kind: Kind) -> Vec<u32> {
        (0..self.buckets)
            .map(|b| self.cells[self.idx(role, zone, keys, b, kind)].load(Ordering::Relaxed))
            .collect()
    }
}

struct Config {
    response_timeout: Option<Duration>,
    duration_ms: u64,
    deadline_ms: u64,
    connections_per_zone: usize,
    seed: String,
    keys_dir: String,
    out: String,
}

impl Config {
    fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        let arg = |name: &str| -> Option<String> {
            args.iter()
                .position(|a| a == name)
                .and_then(|i| args.get(i + 1).cloned())
        };
        let parse = |name: &str, default: u64| -> u64 {
            arg(name).map_or(default, |v| {
                v.parse().unwrap_or_else(|_| panic!("{name} {v}"))
            })
        };
        Self {
            response_timeout: arg("--response-timeout-ms")
                .filter(|v| v != "none")
                .map(|v| Duration::from_millis(v.parse().expect("--response-timeout-ms"))),
            duration_ms: parse("--duration-s", 60) * 1000,
            deadline_ms: parse("--deadline-ms", 1000),
            connections_per_zone: parse("--connections-per-zone", 7) as usize,
            seed: arg("--seed").unwrap_or_else(|| "redis://127.0.0.1:7101/".to_owned()),
            keys_dir: arg("--keys-dir").expect("--keys-dir"),
            out: arg("--out").expect("--out"),
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_millis() as u64
}

fn read_keys(dir: &str, name: &str) -> Arc<Vec<String>> {
    let text = std::fs::read_to_string(format!("{dir}/{name}")).expect("key file");
    Arc::new(text.lines().map(str::to_owned).collect())
}

fn main() {
    env_logger::Builder::from_default_env()
        .format(|buf, record| {
            writeln!(buf, "LOG {} {} {}", now_ms(), record.level(), record.args())
        })
        .init();
    let cfg = Config::from_args();
    let key_sets = [
        read_keys(&cfg.keys_dir, "keys_a.txt"),
        read_keys(&cfg.keys_dir, "keys_b.txt"),
    ];
    let per_zone = cfg.connections_per_zone;

    let mut connections = Vec::new();
    for zone in ZONES {
        let mut builder = redis::cluster::ClusterClient::builder(vec![cfg.seed.as_str()])
            .read_from_zonal_replicas(zone);
        if let Some(timeout) = cfg.response_timeout {
            builder = builder.response_timeout(timeout);
        }
        let client = builder.build().expect("cluster client");
        for _ in 0..per_zone {
            connections.push(client.get_connection().expect("cluster connection"));
        }
    }

    let buckets = (cfg.duration_ms / BUCKET_MS) as usize + 1;
    let counts = Arc::new(Counts::new(buckets));
    let inflight: Arc<Vec<AtomicU64>> =
        Arc::new((0..connections.len()).map(|_| AtomicU64::new(0)).collect());
    let stop = Arc::new(AtomicBool::new(false));
    let (done_tx, done_rx) = std::sync::mpsc::channel::<()>();
    let start_ms = now_ms();
    println!("READY {start_ms}");
    std::io::stdout().flush().expect("stdout");

    for (thread, mut conn) in connections.into_iter().enumerate() {
        let zone = thread / per_zone;
        let role = if thread % per_zone + 1 == per_zone {
            Role::Write
        } else {
            Role::Read
        };
        let (counts, inflight, stop, key_sets, done_tx) = (
            counts.clone(),
            inflight.clone(),
            stop.clone(),
            key_sets.clone(),
            done_tx.clone(),
        );
        let deadline_ms = cfg.deadline_ms;
        std::thread::spawn(move || {
            let mut rng = rand::rng();
            while !stop.load(Ordering::Relaxed) {
                let set = rng.random_range(0..KEY_SETS);
                let key = &key_sets[set][rng.random_range(0..key_sets[set].len())];
                let begin = now_ms();
                inflight[thread].store(begin, Ordering::Relaxed);
                let found = match role {
                    Role::Read => redis::cmd("GET")
                        .arg(key)
                        .query::<Option<Vec<u8>>>(&mut conn)
                        .map(|v| v.is_some()),
                    Role::Write => redis::cmd("SET")
                        .arg(key)
                        .arg(VALUE)
                        .query::<()>(&mut conn)
                        .map(|()| true),
                };
                let end = now_ms();
                inflight[thread].store(0, Ordering::Relaxed);
                let bucket = ((end - start_ms) / BUCKET_MS) as usize;
                if bucket < buckets {
                    let kind = match found {
                        Ok(false) => Kind::Missing,
                        Ok(true) if end - begin <= deadline_ms => Kind::Ok,
                        Ok(true) => Kind::Late,
                        Err(_) => Kind::Err,
                    };
                    counts.add(role, zone, set, bucket, kind);
                }
                std::thread::sleep(Duration::from_millis(if role == Role::Write {
                    5
                } else {
                    2
                }));
            }
            let _ = done_tx.send(());
        });
    }
    drop(done_tx);

    // Sample threads stuck past the deadline: per zone, readers then writers.
    let threads = per_zone * ZONES.len();
    let mut hung_samples = Vec::new();
    while now_ms() - start_ms < cfg.duration_ms {
        let now = now_ms();
        let mut hung = [0u32; 6];
        for (thread, begin) in inflight.iter().enumerate() {
            let begin = begin.load(Ordering::Relaxed);
            if begin != 0 && now.saturating_sub(begin) > cfg.deadline_ms {
                let writer = thread % per_zone + 1 == per_zone;
                hung[thread / per_zone + if writer { ZONES.len() } else { 0 }] += 1;
            }
        }
        hung_samples.push(json!([now - start_ms, hung]));
        std::thread::sleep(Duration::from_millis(100));
    }
    stop.store(true, Ordering::Relaxed);

    // Threads that are not stuck finish within one command.
    let mut finished = 0;
    while finished < threads && done_rx.recv_timeout(Duration::from_secs(2)).is_ok() {
        finished += 1;
    }

    let series = |role: Role, zone: usize, set: usize| {
        json!({
            "ok": counts.series(role, zone, set, Kind::Ok),
            "late": counts.series(role, zone, set, Kind::Late),
            "err": counts.series(role, zone, set, Kind::Err),
            "missing": counts.series(role, zone, set, Kind::Missing),
        })
    };
    let zones: serde_json::Map<String, serde_json::Value> = ZONES
        .iter()
        .enumerate()
        .map(|(z, name)| {
            (
                (*name).to_owned(),
                json!({
                    "reads_a": series(Role::Read, z, 0), "reads_other": series(Role::Read, z, 1),
                    "writes_a": series(Role::Write, z, 0), "writes_other": series(Role::Write, z, 1),
                }),
            )
        })
        .collect();
    let result = json!({
        "start_ms": start_ms,
        "bucket_ms": BUCKET_MS,
        "deadline_ms": cfg.deadline_ms,
        "response_timeout_ms": cfg.response_timeout.map(|d| d.as_millis() as u64),
        "connections_per_zone": per_zone,
        "threads_stuck_at_end": threads - finished,
        "zones": zones,
        "hung": hung_samples,
    });
    std::fs::write(&cfg.out, serde_json::to_vec(&result).expect("json")).expect("write result");
    println!("DONE {finished}/{threads} threads finished");
    std::io::stdout().flush().expect("stdout");
    // Threads still blocked inside the client cannot be joined.
    std::process::exit(0);
}
