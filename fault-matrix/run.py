#!/usr/bin/env -S uv run --script
#
# /// script
# requires-python = ">=3.12"
# dependencies = ["redis>=5,<7", "httpx"]
# ///
"""Cluster fault regression matrix for the zonal read client.

Each scenario boots a fresh local Valkey cluster shaped like a production shard
whose primary takes one zone's slot (so that zone has a single replica), drives
it with the `fault-matrix` load harness, injects one real fault, and checks the
outcome against the scenario's expectations. See README.md.

    fault-matrix/run.py                      # every scenario
    fault-matrix/run.py resync failover      # a subset
    fault-matrix/run.py --list               # scenario names and checks
"""

from __future__ import annotations

import argparse
import json
import math
import os
import shutil
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx
import redis
from redis.backoff import NoBackoff
from redis.cluster import RedisCluster
from redis.crc import key_slot
from redis.retry import Retry

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
HARNESS = REPO / "target/release/fault-matrix"

ZONES = ["us-east-1a", "us-east-1b", "us-east-1c"]
# Shard A: primary and the lone replica in us-east-1a (7102, the "victim");
# shard B: primary in us-east-1b; shard C (only some scenarios): primary in 1c.
AZ = {
    7101: "us-east-1a", 7102: "us-east-1a", 7103: "us-east-1b", 7104: "us-east-1b", 7105: "us-east-1c", 7106: "us-east-1c",
    7107: "us-east-1b", 7108: "us-east-1a", 7109: "us-east-1a", 7110: "us-east-1b", 7111: "us-east-1c", 7112: "us-east-1c",
    7113: "us-east-1c", 7114: "us-east-1a", 7115: "us-east-1a", 7116: "us-east-1b", 7117: "us-east-1b", 7118: "us-east-1c",
    7120: "us-east-1a",
}
SHARD_A, SHARD_B, SHARD_C = [7101, 7102, 7103, 7104, 7105, 7106], [7107, 7108, 7109, 7110, 7111, 7112], [7113, 7114, 7115, 7116, 7117, 7118]
VICTIM, VICTIM_ANNOUNCED = 7102, 7202
TOXI_PORT = 8474
FAULT_AT = 10.0
BASELINE = (3.0, 9.0)
PRELOAD_KEYS = 600_000
# Slows a replica's RDB load so a full resync's LOADING phase lasts ~30 s.
KEY_LOAD_DELAY_US = 110


def tool(env: str, *names: str) -> str:
    if os.environ.get(env):
        return os.environ[env]
    for name in names:
        if found := shutil.which(name):
            return found
    sys.exit(f"{env} not set and none of {names} on PATH (see fault-matrix/README.md)")


SERVER = tool("VALKEY_SERVER", "valkey-server", "redis-server")
CLI = tool("VALKEY_CLI", "valkey-cli", "redis-cli")
TOXIPROXY = tool("TOXIPROXY_SERVER", "toxiproxy-server")


def node(port: int, timeout: float | None) -> redis.Redis:
    # No client-side retries: a stopped node must fail at once so loops over
    # nodes cannot stall a scenario's timeline.
    return redis.Redis(host="127.0.0.1", port=port, socket_timeout=timeout, socket_connect_timeout=timeout,
                       retry=Retry(NoBackoff(), 0))


def cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([CLI, *args], capture_output=True, text=True, check=False)


# ---------------------------------------------------------------- cluster ---

@dataclass(frozen=True)
class Boot:
    """How a scenario's cluster starts."""
    shards: int            # slot owners at boot: 2 (A, B) or 3 (A, B, C)
    failover: bool         # short node timeout; only 7103 (shard A, 1b) may be promoted
    spare_shard_c: bool    # start shard C's nodes outside the cluster, for a scenario to add


class Cluster:
    def __init__(self, data: Path, boot: Boot) -> None:
        self.data = data
        self.boot = boot
        self.toxiproxy: subprocess.Popen[bytes] | None = None

    def conf(self, port: int, extra: str) -> Path:
        d = self.data / str(port)
        d.mkdir(parents=True, exist_ok=True)
        lines = [
            f"port {port}", "bind 127.0.0.1", "protected-mode no", f"dir {d}", f"logfile {d}/valkey.log",
            "daemonize yes", "cluster-enabled yes", "cluster-config-file nodes.conf",
            f"cluster-node-timeout {5000 if self.boot.failover else 120000}",
            f"cluster-replica-no-failover {'no' if self.boot.failover and port == 7103 else 'yes'}",
            # Keep the topology as booted: Valkey would otherwise move replicas
            # to a primary that looks orphaned.
            "cluster-allow-replica-migration no",
            f"availability-zone {AZ[port]}", "enable-debug-command yes", 'save ""', "appendonly no",
            # Small replication buffers: a replica that stalls while its primary
            # takes writes falls out of the backlog and needs a full resync.
            "repl-backlog-size 1mb", "client-output-buffer-limit replica 4mb 2mb 10",
            "repl-diskless-sync yes", "repl-diskless-sync-delay 0", "repl-diskless-load disabled",
            "replica-serve-stale-data yes", "busy-reply-threshold 1000",
        ]
        if port == VICTIM:
            lines += [f"cluster-announce-port {VICTIM_ANNOUNCED}", f"cluster-announce-bus-port {VICTIM + 10000}"]
        path = d / "valkey.conf"
        path.write_text("\n".join(lines) + "\n" + extra)
        return path

    def start_node(self, port: int, extra: str) -> None:
        subprocess.run([SERVER, str(self.conf(port, extra))], check=True)
        for _ in range(100):
            try:
                if node(port, 0.5).ping():
                    return
            except redis.RedisError:
                time.sleep(0.05)
        raise RuntimeError(f"node {port} did not start")

    def add_replica(self, port: int, primary: int) -> None:
        primary_id = node(primary, 2).execute_command("CLUSTER", "MYID").decode()
        out = cli("--cluster", "add-node", f"127.0.0.1:{port}", f"127.0.0.1:{primary}", "--cluster-slave", "--cluster-master-id", primary_id)
        if out.returncode:
            raise RuntimeError(f"add-node {port}: {out.stdout[-400:]}")

    def start(self) -> None:
        stop_all()
        shutil.rmtree(self.data, ignore_errors=True)
        self.data.mkdir(parents=True)
        self.toxiproxy = subprocess.Popen([TOXIPROXY, "-host", "127.0.0.1", "-port", str(TOXI_PORT)],
                                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            try:
                httpx.post(f"http://127.0.0.1:{TOXI_PORT}/proxies", json={
                    "name": "victim", "listen": f"127.0.0.1:{VICTIM_ANNOUNCED}", "upstream": f"127.0.0.1:{VICTIM}"}).raise_for_status()
                break
            except httpx.HTTPError:
                time.sleep(0.05)
        ports = SHARD_A + SHARD_B + (SHARD_C if self.boot.shards == 3 or self.boot.spare_shard_c else [])
        for port in ports:
            self.start_node(port, "")
        primaries = [7101, 7107] + ([7113] if self.boot.shards == 3 else [])
        out = cli("--cluster", "create", *[f"127.0.0.1:{p}" for p in primaries], "--cluster-replicas", "0", "--cluster-yes")
        if out.returncode:
            raise RuntimeError(f"cluster create: {out.stdout[-400:]}")
        time.sleep(2)
        for primary, shard in [(7101, SHARD_A), (7107, SHARD_B)] + ([(7113, SHARD_C)] if self.boot.shards == 3 else []):
            for port in shard[1:]:
                self.add_replica(port, primary)
        if self.boot.spare_shard_c:
            out = cli("--cluster", "add-node", "127.0.0.1:7113", "127.0.0.1:7101")
            if out.returncode:
                raise RuntimeError(f"add-node 7113: {out.stdout[-400:]}")
            time.sleep(2)
            for port in SHARD_C[1:]:
                self.add_replica(port, 7113)
        want = 6 * (self.boot.shards + (1 if self.boot.spare_shard_c else 0))
        for _ in range(120):
            info = node(7101, 2).execute_command("CLUSTER", "INFO").decode()
            nodes = node(7101, 2).execute_command("CLUSTER", "NODES").decode().strip().splitlines()
            if "cluster_state:ok" in info and len(nodes) >= want and all("handshake" not in n for n in nodes):
                return
            time.sleep(0.5)
        raise RuntimeError("cluster did not converge")


def stop_all() -> None:
    for port in AZ:
        try:
            node(port, 0.5).execute_command("SHUTDOWN", "NOSAVE")
        except redis.RedisError:
            pass
    subprocess.run(["pkill", "-f", f"{TOXIPROXY} -host 127.0.0.1 -port {TOXI_PORT}"], check=False)
    time.sleep(0.5)


def preload(out: Path) -> None:
    rc = RedisCluster(host="127.0.0.1", port=7101)
    a_slots: set[int] = set()
    for start, end, primary, *_ in node(7101, 2).execute_command("CLUSTER", "SLOTS"):
        if int(primary[1]) == 7101:
            a_slots.update(range(int(start), int(end) + 1))
    in_a: list[str] = []
    other: list[str] = []
    for base in range(0, PRELOAD_KEYS, 5000):
        pipe = rc.pipeline(transaction=False)
        for i in range(base, min(PRELOAD_KEYS, base + 5000)):
            key = f"k:{i}"
            pipe.set(key, b"v" * 64)
            (in_a if key_slot(key.encode()) in a_slots else other).append(key)
        pipe.execute()
    rc.close()
    (out / "keys_a.txt").write_text("\n".join(in_a))
    (out / "keys_b.txt").write_text("\n".join(other))


def victim_settled() -> bool:
    info = node(VICTIM, 1).info("all")
    if info.get("loading") != 0 or info.get("master_link_status") != "up":
        return False
    for port in AZ:
        try:
            slots = node(port, 1).execute_command("CLUSTER", "SLOTS")
        except redis.RedisError:
            continue
        if slots and not any(int(n[1]) == VICTIM_ANNOUNCED for entry in slots for n in entry[2:]):
            return False
    return True


# ------------------------------------------------------------------- run ---

class Clock:
    def __init__(self, start_ms: int) -> None:
        self.start_ms = start_ms
        self.events: list[dict] = []

    def now(self) -> float:
        return time.time() - self.start_ms / 1000

    def at(self, t: float) -> None:
        time.sleep(max(0.0, t - self.now()))

    def mark(self, event: str) -> None:
        self.events.append({"t": round(self.now(), 2), "event": event})
        print(f"    {self.now():6.1f}s {event}", flush=True)

    def wait_until(self, deadline: float, check: Callable[[], bool], event: str) -> bool:
        while self.now() < deadline:
            try:
                if check():
                    self.mark(event)
                    return True
            except redis.RedisError:
                pass
            time.sleep(0.1)
        return False


def monitor(port: int, clock: Clock, stop: threading.Event, out: list[dict]) -> None:
    client = node(port, 0.3)
    while not stop.is_set():
        t = round(clock.now(), 2)
        try:
            info = client.info("all")
            out.append({"t": t, "get": int((info.get("cmdstat_get") or {}).get("calls", 0)),
                        "role": info.get("role"), "loading": info.get("loading"), "link": info.get("master_link_status")})
        except redis.RedisError:
            out.append({"t": t, "down": True})
            client = node(port, 0.3)
        stop.wait(0.25)


@dataclass
class Run:
    harness: dict
    samples: dict[str, list[dict]]
    events: list[dict]
    breaker_log: list[str]
    duration: float

    def event(self, name: str) -> float:
        for e in self.events:
            if e["event"] == name:
                return float(e["t"])
        return math.nan

    def _buckets(self, role: str, zones: list[str], kind: str) -> list[int]:
        keys = ["reads_a", "reads_other"] if role == "reads" else ["writes_a", "writes_other"]
        n = len(self.harness["zones"][ZONES[0]][keys[0]][kind])
        return [sum(self.harness["zones"][z][k][kind][i] for z in zones for k in keys) for i in range(n)]

    def count(self, role: str, zones: list[str], kind: str, t0: float, t1: float) -> int:
        b = self.harness["bucket_ms"] / 1000
        series = self._buckets(role, zones, kind)
        return sum(series[int(t0 / b):int(t1 / b)])

    def baseline(self, role: str, zones: list[str]) -> float:
        return self.count(role, zones, "ok", *BASELINE) / (BASELINE[1] - BASELINE[0])

    def lost_pct(self, role: str, zones: list[str], t0: float, t1: float) -> float:
        expected = self.baseline(role, zones) * (t1 - t0)
        return max(0.0, 100 * (1 - self.count(role, zones, "ok", t0, t1) / expected)) if expected else 0.0

    def recovery_s(self, role: str, zones: list[str], after: float) -> float:
        """Seconds from `after` until the 2 s rolling rate of on-time results is
        back to 90% of baseline; inf if it never is before the run ends."""
        base = self.baseline(role, zones)
        t = after
        while t + 2 <= self.duration - 1:
            if self.count(role, zones, "ok", t, t + 2) / 2 >= 0.9 * base:
                return round(t - after, 2)
            t += 0.25
        return math.inf

    def hung_thread_s(self, t0: float, t1: float) -> float:
        return round(sum(sum(h) for t, h in self.harness["hung"] if t0 * 1000 <= t < t1 * 1000) * 0.1, 1)

    def node_gets(self, port: int, t0: float, t1: float) -> int:
        vals = [s["get"] for s in self.samples.get(str(port), []) if "get" in s and t0 <= s["t"] <= t1]
        return vals[-1] - vals[0] if len(vals) > 1 else 0

    def first_sample(self, port: int, t0: float, pred: Callable[[dict], bool]) -> float:
        for s in self.samples.get(str(port), []):
            if s["t"] >= t0 and pred(s):
                return float(s["t"])
        return math.nan


# ---------------------------------------------------------------- checks ---

@dataclass(frozen=True)
class Check:
    """One expectation. `expect` is "pass" (failing is a regression),
    "known_gap" (fails today; passing means the gap closed) or "info"
    (recorded, never fails)."""
    name: str
    metric: Callable[[Run], float]
    op: str
    limit: float
    unit: str
    expect: str


def evaluate(check: Check, run: Run) -> dict:
    try:
        value = float(check.metric(run))
    except Exception as err:  # noqa: BLE001 - a broken metric is reported, not fatal
        return {"check": check.name, "status": "ERROR", "value": None, "detail": repr(err)}
    if math.isnan(value):
        return {"check": check.name, "status": "ERROR", "value": None, "detail": "not measurable (event missing)"}
    ok = value <= check.limit if check.op == "<=" else value >= check.limit
    status = {"pass": "PASS" if ok else "FAIL", "known_gap": "GAP CLOSED" if ok else "KNOWN GAP", "info": "INFO"}[check.expect]
    return {"check": check.name, "status": status, "value": value, "op": check.op, "limit": check.limit, "unit": check.unit}


# -------------------------------------------------------------- scenarios ---

FaultFn = Callable[[Clock, Cluster], None]


@dataclass(frozen=True)
class Scenario:
    name: str
    about: str
    boot: Boot
    duration: float
    fault: FaultFn
    checks: list[Check]


TWO = Boot(shards=2, failover=False, spare_shard_c=False)
ALL = ZONES
A1 = ["us-east-1a"]


def toxic(name: str, latency_ms: int | None) -> None:
    base = f"http://127.0.0.1:{TOXI_PORT}/proxies/victim/toxics"
    if latency_ms is None:
        httpx.delete(f"{base}/{name}").raise_for_status()
    else:
        httpx.post(base, json={"name": name, "type": "latency", "stream": "downstream",
                               "attributes": {"latency": latency_ms, "jitter": latency_ms // 10}}).raise_for_status()


def fault_resync(c: Clock, cluster: Cluster) -> None:
    def sleeper() -> None:
        node(VICTIM, None).execute_command("DEBUG", "SLEEP", 10)
        c.mark("victim_awake")
    c.at(FAULT_AT)
    threading.Thread(target=sleeper, daemon=True).start()
    c.mark("victim_stalled")
    time.sleep(0.3)
    keys = (cluster.data.parent / "keys_a.txt").read_text().split("\n")[:4000]
    pipe = node(7101, 30).pipeline(transaction=False)
    for key in keys:
        pipe.set(key, b"x" * 10_000)
    pipe.execute()
    c.mark("primary_took_40mb_of_writes")


def latency_fault(ms: int) -> FaultFn:
    def fault(c: Clock, cluster: Cluster) -> None:
        c.at(FAULT_AT); toxic("lat", ms); c.mark(f"latency_plus_{ms}ms")
        c.at(FAULT_AT + 30); toxic("lat", None); c.mark("latency_removed")
    return fault


def fault_cpu(c: Clock, cluster: Cluster) -> None:
    stop = threading.Event()

    def burner() -> None:
        r = node(VICTIM, 30)
        while not stop.is_set():
            r.execute_command("EVAL_RO", "local x = 0 for i = 1, 3000000 do x = x + i end return x", 0)

    c.at(FAULT_AT)
    threads = [threading.Thread(target=burner, daemon=True) for _ in range(8)]
    for th in threads:
        th.start()
    c.mark("cpu_flood_on")
    c.at(FAULT_AT + 30); stop.set(); c.mark("cpu_flood_off")


def fault_busy(c: Clock, cluster: Cluster) -> None:
    def busy() -> None:
        try:
            node(VICTIM, None).execute_command("EVAL_RO", "while true do end", 0)
        except redis.RedisError:
            pass
    c.at(FAULT_AT); threading.Thread(target=busy, daemon=True).start(); c.mark("busy_script_started")
    c.at(FAULT_AT + 15); node(VICTIM, 5).execute_command("SCRIPT", "KILL"); c.mark("busy_script_killed")


def kill_primary(c: Clock, cluster: Cluster, restart_at: float, promote: bool, duration: float) -> None:
    pid = int(node(7101, 1).info("server")["process_id"])
    c.at(FAULT_AT); os.kill(pid, 9); c.mark("primary_killed")
    if promote:
        c.wait_until(restart_at - 1, lambda: node(7103, 1).info("replication")["role"] == "master", "replica_promoted")
    c.at(restart_at)
    subprocess.run([SERVER, str(cluster.conf(7101, f"key-load-delay {KEY_LOAD_DELAY_US}\nloading-process-events-interval-bytes 1024\n"))], check=True)
    c.mark("old_primary_restarted_empty")
    if promote:
        c.wait_until(duration - 2, lambda: node(7101, 1).info("all").get("loading") == 1, "old_primary_loading_as_replica")


def fault_failover(c: Clock, cluster: Cluster) -> None:
    kill_primary(c, cluster, FAULT_AT + 30, True, 80)


def fault_primary_wipe(c: Clock, cluster: Cluster) -> None:
    kill_primary(c, cluster, FAULT_AT + 15, False, 70)


def rebalance(c: Clock, *extra: str) -> None:
    out = subprocess.run([CLI, "--cluster", "rebalance", "127.0.0.1:7101", "--cluster-pipeline", "50", *extra],
                         capture_output=True, text=True, check=False)
    c.mark("rebalance_done" if out.returncode == 0 else "rebalance_failed")


def fault_reshard_up(c: Clock, cluster: Cluster) -> None:
    c.at(FAULT_AT); c.mark("rebalance_started")
    rebalance(c, "--cluster-use-empty-masters")


def drain_shard_c(terminate: bool) -> FaultFn:
    def fault(c: Clock, cluster: Cluster) -> None:
        c_id = node(7113, 1).execute_command("CLUSTER", "MYID").decode()
        c.at(FAULT_AT); c.mark("rebalance_started")
        rebalance(c, "--cluster-weight", f"{c_id}=0")
        for port in SHARD_C[1:] + SHARD_C[:1]:
            cli("--cluster", "del-node", "127.0.0.1:7101", node(port, 1).execute_command("CLUSTER", "MYID").decode())
            if terminate:
                try:
                    node(port, 2).execute_command("SHUTDOWN", "NOSAVE")
                except redis.RedisError:
                    pass
        c.mark("shard_c_removed")
    return fault


def fault_add_replica(c: Clock, cluster: Cluster) -> None:
    c.at(FAULT_AT)
    cluster.start_node(7120, f"key-load-delay {KEY_LOAD_DELAY_US}\nloading-process-events-interval-bytes 1024\n")
    cluster.add_replica(7120, 7101)
    c.mark("replica_added")
    c.wait_until(85, lambda: (lambda i: i.get("loading") == 0 and i.get("master_link_status") == "up")(node(7120, 1).info("all")), "new_replica_synced")


def fault_remove_replica(c: Clock, cluster: Cluster) -> None:
    victim_id = node(VICTIM, 1).execute_command("CLUSTER", "MYID").decode()
    c.at(FAULT_AT)
    httpx.post(f"http://127.0.0.1:{TOXI_PORT}/proxies/victim", json={"enabled": False}).raise_for_status()
    try:
        node(VICTIM, 2).execute_command("SHUTDOWN", "NOSAVE")
    except redis.RedisError:
        pass
    for _ in range(3):
        for port in AZ:
            if port != VICTIM:
                try:
                    node(port, 0.5).execute_command("CLUSTER", "FORGET", victim_id)
                except redis.RedisError:
                    pass  # not running, or already forgot it
        time.sleep(0.5)
    c.mark("replica_removed")


def resync_done(r: Run) -> float:
    start = r.first_sample(VICTIM, FAULT_AT, lambda s: s.get("loading") == 1)
    return r.first_sample(VICTIM, start, lambda s: s.get("loading") == 0 and s.get("link") == "up")


SCENARIOS = [
    Scenario("resync", "The lone us-east-1a replica of shard A stalls for 10 s while its primary takes 40 MB of writes, so it falls out of the backlog and comes back with a full resync (~30 s of LOADING).",
             TWO, 90, fault_resync, [
                 Check("us-east-1a reads lost, stall through recovery", lambda r: r.lost_pct("reads", A1, FAULT_AT, resync_done(r) + 5), "<=", 5, "%", "pass"),
                 Check("hung thread-seconds, all zones", lambda r: r.hung_thread_s(FAULT_AT, r.duration), "<=", 2, "thread-s", "pass"),
             ]),
    Scenario("slow_over_timeout", "The lone us-east-1a replica answers 400 ms late for 30 s, over the client response timeout.",
             TWO, 45, latency_fault(400), [
                 Check("us-east-1a reads lost while slow", lambda r: r.lost_pct("reads", A1, FAULT_AT, FAULT_AT + 30), "<=", 5, "%", "pass"),
             ]),
    Scenario("slow_under_timeout", "The lone us-east-1a replica answers 100 ms late for 30 s, under the client response timeout.",
             TWO, 45, latency_fault(100), [
                 Check("us-east-1a reads lost while slow", lambda r: r.lost_pct("reads", A1, FAULT_AT, FAULT_AT + 30), "<=", 20, "%", "known_gap"),
             ]),
    Scenario("cpu_saturation", "Eight clients run CPU-heavy Lua on the lone us-east-1a replica for 30 s, saturating its main thread.",
             TWO, 45, fault_cpu, [
                 Check("us-east-1a reads lost while saturated", lambda r: r.lost_pct("reads", A1, FAULT_AT, FAULT_AT + 30), "<=", 20, "%", "known_gap"),
             ]),
    Scenario("busy_script", "A script that never finishes holds the lone us-east-1a replica for 15 s; after 1 s it answers BUSY.",
             TWO, 40, fault_busy, [
                 # Reads already waiting on the replica when it blocks get BUSY
                 # before the breaker opens: one per reader thread at most.
                 Check("read errors", lambda r: r.count("reads", ALL, "err", FAULT_AT, FAULT_AT + 20), "<=", 50, "reads", "pass"),
                 Check("us-east-1a reads lost while blocked", lambda r: r.lost_pct("reads", A1, FAULT_AT, FAULT_AT + 15), "<=", 10, "%", "pass"),
             ]),
    Scenario("failover", "Shard A's primary (us-east-1a) is killed; the us-east-1b replica is promoted; the old primary comes back empty 30 s later and rejoins as a replica.",
             Boot(shards=3, failover=True, spare_shard_c=False), 80, fault_failover, [
                 Check("reads lost, all zones", lambda r: r.lost_pct("reads", ALL, FAULT_AT, 75), "<=", 5, "%", "pass"),
                 Check("writes back to 90% after promotion", lambda r: r.recovery_s("writes", ALL, r.event("replica_promoted")), "<=", 10, "s", "known_gap"),
             ]),
    Scenario("primary_wipe", "Two primaries, so a dead one can never be marked FAIL: shard A's primary is killed and restarted empty 15 s later, and its replicas copy the empty dataset.",
             Boot(shards=2, failover=True, spare_shard_c=False), 70, fault_primary_wipe, [
                 Check("reads that returned nothing for existing keys", lambda r: 100 * r.count("reads", ALL, "missing", FAULT_AT, 65) / max(1, sum(r.count("reads", ALL, k, FAULT_AT, 65) for k in ("ok", "late", "missing"))), "<=", 0, "%", "info"),
                 Check("hung thread-seconds, all zones", lambda r: r.hung_thread_s(FAULT_AT, r.duration), "<=", 0, "thread-s", "info"),
             ]),
    Scenario("reshard_up", "An empty third shard joins and `valkey-cli --cluster rebalance` moves a third of the slots to it under load.",
             Boot(shards=2, failover=False, spare_shard_c=True), 80, fault_reshard_up, [
                 # Migration redirects cost a few percent and vary run to run.
                 Check("reads lost, all zones", lambda r: r.lost_pct("reads", ALL, FAULT_AT, 75), "<=", 10, "%", "pass"),
                 Check("writes lost, all zones", lambda r: r.lost_pct("writes", ALL, FAULT_AT, 75), "<=", 10, "%", "pass"),
                 Check("reads that returned nothing", lambda r: r.count("reads", ALL, "missing", 0, 80), "<=", 0, "reads", "pass"),
                 Check("read errors", lambda r: r.count("reads", ALL, "err", 0, 80), "<=", 0, "reads", "pass"),
             ]),
    Scenario("reshard_down", "Shard C's slots move to A and B, then its six nodes are removed and terminated, as ElastiCache deletes them.",
             Boot(shards=3, failover=False, spare_shard_c=False), 80, drain_shard_c(True), [
                 Check("read errors after removal", lambda r: r.count("reads", ALL, "err", r.event("shard_c_removed") + 2, 79), "<=", 0, "reads", "pass"),
                 Check("reads lost after removal", lambda r: r.lost_pct("reads", ALL, r.event("shard_c_removed") + 2, 79), "<=", 5, "%", "pass"),
                 Check("writes lost after removal", lambda r: r.lost_pct("writes", ALL, r.event("shard_c_removed") + 5, 79), "<=", 2, "%", "known_gap"),
             ]),
    Scenario("reshard_down_reset", "Same drain, but the removed nodes stay up after CLUSTER RESET (what `valkey-cli --cluster del-node` leaves) and answer CLUSTERDOWN.",
             Boot(shards=3, failover=False, spare_shard_c=False), 80, drain_shard_c(False), [
                 Check("threads stuck when the run ends", lambda r: r.harness["threads_stuck_at_end"], "<=", 0, "threads", "known_gap"),
                 Check("reads lost after removal", lambda r: r.lost_pct("reads", ALL, r.event("shard_c_removed") + 2, 79), "<=", 5, "%", "known_gap"),
             ]),
    Scenario("add_replica", "A second us-east-1a replica joins shard A and fully syncs.",
             TWO, 90, fault_add_replica, [
                 Check("reads served by the new replica after it synced", lambda r: r.node_gets(7120, r.event("new_replica_synced"), 89), ">=", 1, "reads", "known_gap"),
             ]),
    Scenario("remove_replica", "The lone us-east-1a replica of shard A is shut down and forgotten by every node; its address refuses connections.",
             TWO, 45, fault_remove_replica, [
                 Check("us-east-1a reads lost after removal", lambda r: r.lost_pct("reads", A1, FAULT_AT, 44), "<=", 2, "%", "pass"),
                 Check("read errors", lambda r: r.count("reads", ALL, "err", FAULT_AT, 44), "<=", 100, "reads", "pass"),
             ]),
]


def run_scenario(sc: Scenario, out: Path, response_timeout: str) -> tuple[Run | None, str]:
    out.mkdir(parents=True, exist_ok=True)
    cluster = Cluster(out / "data", sc.boot)
    cluster.start()
    preload(out)
    for _ in range(240):
        try:
            if victim_settled():
                break
        except redis.RedisError:
            pass
        time.sleep(0.5)
    else:
        return None, "the us-east-1a replica never settled into the topology"
    node(VICTIM, 2).config_set("key-load-delay", KEY_LOAD_DELAY_US)
    node(VICTIM, 2).config_set("loading-process-events-interval-bytes", 1024)

    log = open(out / "harness.log", "w")
    proc = subprocess.Popen(
        [str(HARNESS), "--duration-s", str(int(sc.duration)), "--response-timeout-ms", response_timeout,
         "--keys-dir", str(out), "--out", str(out / "harness.json")],
        stdout=subprocess.PIPE, stderr=log, text=True, env={"RUST_LOG": "info", "PATH": os.environ.get("PATH", "")})
    line = proc.stdout.readline() if proc.stdout else ""
    if not line.startswith("READY"):
        return None, f"harness did not start: {line!r}"
    clock = Clock(int(line.split()[1]))
    stop = threading.Event()
    samples: dict[str, list[dict]] = {str(p): [] for p in AZ}
    for port in AZ:
        threading.Thread(target=monitor, args=(port, clock, stop, samples[str(port)]), daemon=True).start()

    # Precondition: us-east-1a clients read shard A from the lone 1a replica.
    clock.at(3)
    before = node(VICTIM, 1).info("commandstats").get("cmdstat_get", {}).get("calls", 0)
    clock.at(7)
    rate = (node(VICTIM, 1).info("commandstats").get("cmdstat_get", {}).get("calls", 0) - before) / 4
    if rate < 100:
        proc.kill()
        stop.set()
        return None, f"precondition failed: the us-east-1a replica served {rate:.0f} GET/s before the fault"
    sc.fault(clock, cluster)
    rest = proc.stdout.read() if proc.stdout else ""
    proc.wait()
    stop.set()
    time.sleep(0.3)
    harness = json.loads((out / "harness.json").read_text())
    breaker = [l.strip() for l in open(out / "harness.log") if "circuit breaker" in l]
    run = Run(harness, samples, clock.events, breaker, sc.duration)
    (out / "monitor.json").write_text(json.dumps({"samples": samples, "events": clock.events}))
    for f in ("keys_a.txt", "keys_b.txt"):
        (out / f).unlink(missing_ok=True)
    shutil.rmtree(out / "data", ignore_errors=True)
    return run, rest.strip()


def fmt(v: float | None, unit: str) -> str:
    if v is None:
        return "n/a"
    if math.isinf(v):
        return "never"
    return f"{round(v, 1):g} {unit}" if abs(v) < 1000 else f"{v:,.0f} {unit}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("scenarios", nargs="*", help="scenario names (default: all)")
    ap.add_argument("--list", action="store_true", help="list scenarios and checks")
    ap.add_argument("--response-timeout-ms", default="250", help="client response timeout, or 'none' (default 250)")
    ap.add_argument("--out", type=Path, help="results directory (default fault-matrix/results/<timestamp>)")
    ap.add_argument("--strict", action="store_true", help="also fail when a known gap starts passing")
    args = ap.parse_args()

    if args.list:
        for sc in SCENARIOS:
            print(f"{sc.name}: {sc.about}")
            for ck in sc.checks:
                print(f"    [{ck.expect}] {ck.name} {ck.op} {ck.limit} {ck.unit}")
        return 0
    chosen = [s for s in SCENARIOS if not args.scenarios or s.name in args.scenarios]
    unknown = set(args.scenarios) - {s.name for s in SCENARIOS}
    if unknown:
        sys.exit(f"unknown scenarios: {sorted(unknown)}")

    subprocess.run(["cargo", "build", "--release", "-p", "fault-matrix"], cwd=REPO, check=True)
    # Absolute: daemonized nodes resolve their dir and logfile from /.
    out = (args.out or HERE / "results" / time.strftime("%Y%m%d-%H%M%S")).resolve()
    out.mkdir(parents=True, exist_ok=True)
    report: list[dict] = []
    try:
        for sc in chosen:
            print(f"== {sc.name}", flush=True)
            try:
                run, note = run_scenario(sc, out / sc.name, args.response_timeout_ms)
            except Exception as err:  # noqa: BLE001 - one broken scenario must not hide the rest
                run, note = None, f"{type(err).__name__}: {err}"
                stop_all()
            if run is None:
                print(f"    ERROR {note}", flush=True)
                report.append({"scenario": sc.name, "about": sc.about, "results": [{"check": "scenario setup", "status": "ERROR", "value": None, "detail": note}]})
                continue
            results = [evaluate(ck, run) | {"expect": ck.expect} for ck in sc.checks]
            for r in results:
                print(f"    {r['status']:<10} {r['check']}: {fmt(r['value'], r.get('unit', ''))} (limit {r.get('op', '')} {r.get('limit', '')})", flush=True)
            report.append({"scenario": sc.name, "about": sc.about, "events": run.events, "breaker_events": len(run.breaker_log), "results": results})
    finally:
        stop_all()

    (out / "report.json").write_text(json.dumps({"response_timeout_ms": args.response_timeout_ms, "scenarios": report}, indent=2, default=str))
    lines = ["# Cluster fault regression matrix", "", f"Client response timeout: {args.response_timeout_ms} ms", "",
             "| Scenario | Check | Expect | Result | Value | Limit |", "|---|---|---|---|---|---|"]
    for entry in report:
        for r in entry["results"]:
            lines.append(f"| {entry['scenario']} | {r['check']} | {r.get('expect', '')} | {r['status']} | {fmt(r['value'], r.get('unit', ''))} | {r.get('op', '')} {r.get('limit', '')} |")
    (out / "report.md").write_text("\n".join(lines) + "\n")
    print("\n" + "\n".join(lines))

    statuses = [r["status"] for e in report for r in e["results"]]
    closed = statuses.count("GAP CLOSED")
    if closed:
        print(f"\n{closed} known gap(s) now pass: promote them to 'pass' in fault-matrix/run.py.")
    failed = statuses.count("FAIL") + statuses.count("ERROR") + (closed if args.strict else 0)
    print(f"\nResults in {out}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
