# Cluster fault regression matrix

End-to-end checks of how the sync cluster client with zonal read routing
behaves when a real cluster misbehaves. Each scenario boots a fresh local
Valkey cluster, drives it with a load harness built from this checkout's
`redis` crate, injects one real fault, and checks the result against written
expectations.

It exists because unit tests with mocked servers cannot reproduce a replica
resync, a failover, or a slot migration, and those are where zonal routing
fails in production.

## Run it

Requirements:

- Valkey **9.1 or newer** (`valkey-server` and `valkey-cli`). Nodes use the
  `availability-zone` setting, which zonal routing reads from `INFO SERVER`.
- `toxiproxy-server`, for the latency scenarios.
- `uv`, which installs the runner's Python dependencies.
- Free local ports 7101–7120, 7202 and 8474.

```bash
# With nix:
nix shell nixpkgs#valkey nixpkgs#toxiproxy nixpkgs#uv

fault-matrix/run.py                    # every scenario, about 20 minutes
fault-matrix/run.py resync failover    # a subset
fault-matrix/run.py --list             # scenarios and their checks
fault-matrix/run.py --response-timeout-ms none   # without a response timeout
```

The runner finds binaries on `PATH` (`valkey-server`, falling back to
`redis-server`) or through `VALKEY_SERVER`, `VALKEY_CLI` and
`TOXIPROXY_SERVER`. It builds the harness with
`cargo build --release -p fault-matrix`.

Results go to `fault-matrix/results/<timestamp>/`: `report.md`, `report.json`,
and per scenario the harness output, node samples, fault events and the
client's circuit breaker log lines.

The exit code is non-zero when a `pass` check fails or a scenario cannot run.
`--strict` also fails when a known gap starts passing.

## How a run works

- **Topology.** Two shards of six nodes, or three where the scenario needs it.
  Each node has an availability zone. Shard A's primary and its only
  us-east-1a replica (7102) are both in us-east-1a, so that zone reads shard A
  from a single node. 7102 is reached through toxiproxy (announced as 7202).
  Replica migration is off so the topology stays as booted.
- **Load.** One zonal `ClusterClient` per zone, as a service process builds
  one. Seven sync connections per zone, each on its own thread: six read
  `GET` on random preloaded keys from every shard, one writes `SET`. A thread
  whose current command has run past the 1 s deadline counts as hung.
- **Precondition.** Before the fault, each run checks that us-east-1a clients
  are reading shard A from 7102, and stops with an error otherwise.
- **Fault.** Injected at 10 s. Every fault is a real server action: `DEBUG
  SLEEP`, toxiproxy latency, Lua CPU load, `kill -9`,
  `valkey-cli --cluster rebalance`, node add and remove.

## Expectations

Every check has one of three expectations:

| Expect | Meaning | When it fails | When it passes |
|---|---|---|---|
| `pass` | Behavior the client must keep | `FAIL`: a regression, non-zero exit | `PASS` |
| `known_gap` | A documented weakness the client has today | `KNOWN GAP`: expected | `GAP CLOSED`: tighten it to `pass` |
| `info` | A server-side hazard the client cannot fix, recorded for reference | never | never |

| Scenario | What happens | Expect |
|---|---|---|
| `resync` | Lone 1a replica stalls 10 s, falls out of the backlog, full resync with ~30 s of `LOADING` | pass |
| `slow_over_timeout` | Lone 1a replica 400 ms slow for 30 s | pass |
| `slow_under_timeout` | Lone 1a replica 100 ms slow for 30 s | known gap: no latency-based ejection |
| `cpu_saturation` | Lone 1a replica's main thread saturated for 30 s | known gap: same |
| `busy_script` | Lone 1a replica blocked by a script, answering `BUSY` | pass |
| `failover` | Shard A primary killed, a 1b replica promoted, the old primary rejoins empty | pass |
| `primary_wipe` | A primary restarts empty before it can be failed over; its replicas copy the empty dataset | info |
| `reshard_up` | A new shard joins and a third of the slots migrate to it | pass |
| `reshard_down` | A shard is drained and its nodes terminated | pass |
| `reshard_down_reset` | Same, but the drained nodes stay up answering `CLUSTERDOWN` | pass |
| `add_replica` | A second 1a replica joins shard A | pass |
| `remove_replica` | The lone 1a replica is removed | pass |

`fault-matrix/run.py --list` prints the exact thresholds.

## Changing it

- **A fix closes a known gap:** the run reports `GAP CLOSED`. Change that
  check's `expect` to `"pass"` in `run.py` in the same PR, so the fix is
  protected from then on.
- **A new failure mode:** add a `Scenario` with a fault function and checks.
  Metrics are windows over the run, measured against the 3–9 s baseline:
  `lost_pct`, `count`, `recovery_s`, `hung_thread_s` and `node_gets`. Keep
  faults real server actions rather than mocked replies.
- **Thresholds:** the clients are closed-loop. Each thread issues its next
  command when the last returns, so a slow node shows up as fewer completed
  reads rather than as queued requests. Limits are percentages of the
  pre-fault rate, so they do not depend on machine speed.

## Limits

- One host and loopback networking: no TLS and no cross-zone latency.
- Valkey replicas partially resync after a failover. ElastiCache documents
  that its replicas are wiped and fully resync, so a real failover is harsher.
- Slot migration runs on Valkey 9.1, where replicas know about migrations.
  Older engines are not covered.
- The faults are compressed in time. The client's retry and backoff timings are not.
