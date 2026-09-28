# TeleRM — Distributed Telecommunication Resource Management System

A working distributed operating system for telecom workloads: a global
resource manager, three site nodes (edge-A, edge-B, core-1), and a
traffic generator, talking over length-prefixed JSON on TCP, with
global admission control, pluggable placement policies, local stride
scheduling, **live instance migration (M3)**, **concurrent admission
(M3)**, **write-ahead log + crash recovery (M4)**, **HMAC message
authentication (M4)**, and an operator CLI + HTML dashboard for live
observability (M4). Pure Python standard library — nothing to install.

> **Milestone map**
> - **M1** — Distributed OS foundation: manager/sites/protocol/scheduler (in the initial commit; this README's "M1 design" section below).
> - **M2** — Failure-detection scaffolding (presented; the manager's `_failure_detector_loop` + SUSPECT state + topology/link reservations came out of M2 and remain in place).
> - **M3** — **Fault tolerance: live migration + concurrent admission.** Closes the M1 limitations "no migration", "instances not recovered or migrated", "serialised admission".
> - **M4** — **Manager resilience: WAL + snapshot + crash recovery + HMAC auth + observability.** Closes the M1 limitation "single point of failure: admission stops if the manager process dies".
>
> See `MILESTONES_3_4.md` for a per-change mapping to M3 and M4, plus a list of presentation-ready commands.

## Requirements

- Python 3.9+ (developed/tested on 3.12), standard library only.

## Layout

```
telerm/
  config.json          topology, service catalogue, workload, policy
  manager.py           Global Resource Manager (its own OS process)
                       M3: concurrent admission, _migrate_instance,
                           SUSPECT-site evacuation, resize-driven offload
                       M4: WAL append, snapshot, _recover on startup,
                           HMAC verify on inbound, sign on outbound
  site.py              a site node: stride scheduler + worker pool
                       M3: Instance.snapshot/from_snapshot,
                           _on_migrate_snapshot, _on_migrate_allocate,
                           _on_migrate_complete (no INSTANCE_EXIT)
  generator.py         traffic generator (Poisson arrivals, resizes)
                       M4: --auth-key passthrough
  run_demo.py           orchestrator: spawns manager+sites+generator,
                       collects the report. M4: --auth-key / --recover
  telerm_cli.py        M4 operator CLI: status / topology / instances
                       / migrate / drain / shutdown, --watch for live
                       status, --auth-key for HMAC-secured clusters
  dashboard.py          M4 HTML dashboard generator (self-contained,
                       no external JS/CSS): blocking probability bars,
                       per-site CPU/mem/bw bars, link bars, utilisation
                       time-series SVG line chart, KPI grid
  common/
    protocol.py         length-prefixed JSON over TCP, request/reply helper
                        M3: INSTANCE_MIGRATE / MIGRATE_SNAPSHOT /
                            MIGRATE_ALLOCATE / MIGRATE_COMPLETE message types
                        M4: HMAC-SHA256 sign/verify on every message
    topology.py          graph, shortest-latency path, link bandwidth reservations
    util.py              dominant-share math, Jain's index, sid formatting
    counters.py          per-node message/byte counters
  tests/
    test_core.py          M1 unit tests for pure-logic pieces (no sockets)
    test_m3_m4.py         M3+M4 unit tests: migration snapshot round-trip,
                          migration target picker, WAL replay, snapshot
                          round-trip, HMAC signing/verification
  results/                one subfolder per run, created automatically
                          M4: also contains wal.jsonl, manager_state.json,
                          and dashboard.html (if generated)
```

## Quick start

```bash
cd telerm
python3 run_demo.py --policy edge_first
```

That single command spawns the manager and all three sites as real,
independent OS processes (each on its own TCP port — see
`config.json`), runs the generator for the configured workload, waits
for every admitted instance to finish, shuts everything down cleanly,
and prints a summary:

```
=== TeleRM run summary ===
policy: edge_first
overall blocking probability: 9.5%  (offered=21, blocked=2)
  ...
link utilisation (final):
  edge-A<->core-1      0.0%  (0.0/1000 Mb/s)
  ...
migrations: attempted=2 succeeded=2 failed=0      # M3
admission decision time: mean=0.62ms max=1.90ms over 21 requests
full results in: results/edge_first-seed42-.../
```

Compare policies on the same workload:

```bash
python3 run_demo.py --policy edge_first    --seed 42
python3 run_demo.py --policy least_loaded  --seed 42
python3 run_demo.py --policy best_fit      --seed 42
```

Useful flags: `--duration <s>`, `--rate <arrivals/s>`, `--seed <n>`,
`--results-dir <path>`.

### M3 + M4 flags

```bash
# Run with HMAC authentication on every protocol message (M4)
python3 run_demo.py --policy edge_first --auth-key s3cret

# Recover manager state from a previous run's results dir (M4).
# The new manager replays the WAL on top of the latest snapshot, so
# running instances, site allocations, and counters carry over.
python3 run_demo.py --policy edge_first --recover results/edge_first-seed42-.../
```

### Operator CLI (M4)

```bash
# one-shot status
python3 telerm_cli.py --manager 127.0.0.1:6000 status

# watch status live (refresh every 1s) -- great for presentations
python3 telerm_cli.py --manager 127.0.0.1:6000 status --watch

# topology dump (links + capacities + reservations)
python3 telerm_cli.py --manager 127.0.0.1:6000 topology

# list live instances with their site, demand, path, and latency
python3 telerm_cli.py --manager 127.0.0.1:6000 instances

# force-migrate an instance to a specific target
python3 telerm_cli.py --manager 127.0.0.1:6000 migrate S00007 core-1

# let the manager auto-pick the least-loaded target
python3 telerm_cli.py --manager 127.0.0.1:6000 migrate S00007

# wait for all running instances to finish
python3 telerm_cli.py --manager 127.0.0.1:6000 drain

# shut the whole cluster down cleanly
python3 telerm_cli.py --manager 127.0.0.1:6000 shutdown

# HMAC-secured cluster:
python3 telerm_cli.py --manager 127.0.0.1:6000 --auth-key s3cret status
```

### HTML dashboard (M4)

After a run, generate a self-contained HTML report:

```bash
python3 dashboard.py --results results/edge_first-seed42-.../
# writes results/.../dashboard.html
xdg-open results/edge_first-seed42-.../dashboard.html   # Linux
```

The dashboard includes a blocking-probability bar chart, per-site
CPU/mem/bw bars, link utilisation bars, an SVG utilisation time-series
chart per site, a KPI grid (admission mode, auth, recovery, migrations,
decision time, message counts), and a completed-instances table.

### Running nodes by hand (multi-host / manual mode)

Each node is a standalone script and doesn't need `run_demo.py`. To
run on one host:

```bash
python3 manager.py --config config.json --results-dir results/manual
python3 site.py --site-id edge-A --config config.json
python3 site.py --site-id edge-B --config config.json
python3 site.py --site-id core-1 --config config.json
python3 generator.py --config config.json --results-dir results/manual
```

To run across several machines: edit each site's `host` in
`config.json` to that machine's real address, make sure the listed
ports are reachable, and point every `site.py` / `generator.py`
invocation at the manager's real host with `--manager-host` /
`--manager-port`. The wire protocol (`common/protocol.py`) doesn't
care whether the peer is on `localhost` or across the network.

### Tests

```bash
python3 -m unittest discover -s tests -v
# 21 tests: 10 from M1 + 11 from M3+M4
```

## What's produced per run (`results/<run>/`)

| File | Contents |
|---|---|
| `config_used.json` / `config_used_raw.json` | exact config, argv, Python version, platform, seed — the reproducibility record from design-doc §11 |
| `manager_report.json` | blocking probability overall and per-service (with reasons), decision-time stats, final link utilisation, message/byte counts, **migration stats (M3)**, **admission mode / auth / recovery flags (M4)** |
| `manager_state.json` | **M4 snapshot** of the manager's mutable state (allocation table, site vectors, link reservations, next_sid). Written every `snapshot_interval_s` and on shutdown. |
| `wal.jsonl` | **M4 write-ahead log**: one JSON line per state-changing op (REGISTER, ADMIT, EXIT, RESIZE, MIGRATE, STATUS-flip). Replayable by `manager.py --recover`. |
| `instances_log.json` | per-instance summary (jobs_done, busy_s, latency) as each instance terminates |
| `heartbeat_log.json` | every HEARTBEAT received: slot utilisation, jobs in interval, Jain's fairness index (only while saturated) |
| `utilisation_timeseries.json` | per-second CPU/mem/bw utilisation per site and per link |
| `generator_log.json` / `generator_summary.json` | every REQUEST/RESIZE the generator sent and the reply it got |
| `manager.log`, `site-*.log` | raw stdout/stderr of each subprocess |
| `dashboard.html` *(optional, M4)* | self-contained HTML report generated by `dashboard.py --results <run>` |

## How the design doc's mechanisms map to code

- **Admission control (§5.1)** — `Manager._on_request` in `manager.py`
  checks, per active site: reachability, `path_latency ≤ max_latency`,
  node-vector capacity (`util.fits`), and link bandwidth along the
  shortest-latency path (`Topology.path_has_bandwidth`). All three
  must pass. The DU's 2 ms bound is *not* special-cased anywhere — it
  falls straight out of the latency check, since every link is ≥3 ms
  (`tests/test_core.py::test_du_never_reaches_core_within_2ms`
  demonstrates this).
- **Placement policies (§5.2)** — `Manager._apply_policy`.
  `edge_first` sorts by `(path_latency, dominant_share_after)`; since
  only the ingress site can ever have zero latency, "ingress first"
  falls directly out of "lowest latency first". `least_loaded` and
  `best_fit` sort by dominant share / dominant free share (see below).
- **Reserve-then-ALLOCATE, rollback on failure (§5.1)** — the manager
  reserves the node vector and path bandwidth *before* calling the
  site's `ALLOCATE`, and rolls the reservation back if the site
  doesn't answer `ALLOCATE_OK`.
- **M3 — Concurrent admission.** The M1 design held `self.lock` for
  the entire ALLOCATE round trip ("serialised admission"). M3 splits
  `_on_request` into three steps: (a) short critical section picks the
  candidate + tentatively reserves the node vector + path bandwidth
  (so other concurrent admissions can see the reservation in their
  candidate scan); (b) ALLOCATE round trip with the lock RELEASED so
  multiple admissions can be in flight at once; (c) short critical
  section commits (on `ALLOCATE_OK`) or rolls back. Visible to other
  in-flight admissions via `self._tentative` + the standard
  `site["allocated"]` accounting. Decision time drops ~2x in practice
  (see `MILESTONES_3_4.md`).
- **M3 — Live migration.** `Manager._migrate_instance` orchestrates
  the three-step wire dance:
  1. `INSTANCE_MIGRATE` (manager → source) → source returns a
     `MIGRATE_SNAPSHOT` containing `sid`, `service`, `demand`,
     `holding_s`, `jobs_done`, `busy_s`, `latency_sum_ms`,
     `pass_value`, `expires_at`, `created_at`.
  2. `MIGRATE_ALLOCATE` (manager → target) with that snapshot →
     target re-incarnates the instance via
     `Instance.from_snapshot()`, which restores `pass_value`,
     `jobs_done`, `expires_at` so stride scheduling stays fair after
     migration (a migrated instance that reset `pass_value` to 0
     would starve other instances for a window —
     `test_migrated_instance_keeps_pass_value` covers this).
  3. `MIGRATE_COMPLETE` (manager → source) drops the source's local
     copy *without* reporting `INSTANCE_EXIT` (the manager's
     allocation table has already been updated to point at the
     target; an `INSTANCE_EXIT` here would (incorrectly) delete the
     instance from the manager's view).
  Triggered in two ways: (a) **SUSPECT evacuation** — when the failure
  detector demotes a site to SUSPECT, `_failure_detector_loop` calls
  `_migrate_instance` for every live instance on that site. If the
  source is unreachable (it's dead), the manager skips the source
  round-trips entirely and re-`ALLOCATE`s the instance at the target
  using only the manager's own allocation-table record (sid, service,
  demand, holding_s). This loses per-instance scheduler bookkeeping
  (pass_value, jobs_done) but is the only honest thing to do — the
  source is gone. (b) **Resize-driven offload** — when a RESIZE
  doesn't fit locally, `_on_resize` calls `_pick_migration_target`
  and migrates the instance to a site with the headroom before
  applying the resize, instead of returning `RESIZE_FAIL`.
- **Stride scheduling (§4.4)** — `Site._pick_next_locked` /
  `_worker_loop` in `site.py`: `stride = 10^6 / tickets`, dispatch the
  eligible instance with the smallest `pass_value`, then
  `pass += stride`. A new instance starts at the current minimum pass.
  An instance's in-flight job count is capped at `⌈cpu/1000⌉`.
- **Fairness sampling (§4.4)** — `Site._heartbeat_loop` computes Jain's
  index over `jobs_in_interval / tickets` only when slot utilisation
  exceeds `fairness_saturation_threshold` (default 0.9), matching the
  "only valid under contention" fix recorded in the doc's Week-1 log.
- **Failure detection (§3.2, §8)** — `Manager._failure_detector_loop`
  marks a site `SUSPECT` after `failure_timeout_heartbeats` missed
  heartbeat intervals, excludes it from placement, and (M3) evacuates
  its live instances to healthy sites via `_migrate_instance`'s
  recovery path.
- **M4 — Write-ahead log + snapshot + crash recovery.** Every state-
  changing op is appended to `wal.jsonl` (`Manager._wal_append`).
  Every `snapshot_interval_s` (default 5), the manager writes
  `manager_state.json` (`Manager._snapshot_locked`). On startup with
  `--recover <dir>`, the manager reads the latest snapshot and
  replays the WAL tail to rebuild its in-memory state
  (`Manager._recover` + `_replay_op`). The scenario "kill the
  manager mid-run, restart it, keep serving" is demonstrated in
  `MILESTONES_3_4.md`.
- **M4 — HMAC message authentication.** When `--auth-key` is set on
  manager + sites + generator + CLI, every outbound message is signed
  with HMAC-SHA256 (`protocol._sign`) and every inbound message is
  verified (`protocol._verify`); mismatches produce an `ERROR`
  reply rather than dispatching. Canonical payload:
  `"<len>|<json-body-without-hmac-fields>"` (deterministic by
  sorted-keys JSON). When no `--auth-key` is configured, the protocol
  behaves exactly like M1 — backwards compatible.
- **M4 — Observability CLI.** `telerm_cli.py` exposes
  `status [--watch]`, `topology`, `instances`, `migrate`, `drain`,
  `shutdown`. Designed for live presentation demos — e.g.
  `telerm_cli.py --manager ... migrate S00007 edge-B` triggers an
  M3 migration in real time, and the next `status --watch` frame
  shows the new path/latency and the migrated resources on edge-B.
- **M4 — HTML dashboard.** `dashboard.py --results <run>` emits a
  self-contained `dashboard.html` (inline CSS + SVG, no external
  assets). Useful as a one-page printable summary of a run.
- **Global instance IDs (§2)** — `util.format_sid`, a monotonically
  increasing counter under the manager's lock: `S00001`, `S00002`, ...
- **Communication (§6)** — `common/protocol.py`: 4-byte big-endian
  length prefix + JSON, one request/one reply per exchange.

## Implementation choices worth knowing about

A few places where the design doc leaves room for a concrete decision;
these are the ones this implementation made, and why.

- **Workers are threads, not OS processes.** The design doc says "the
  manager, each site, and each site's worker pool are OS processes."
  Here, the *manager* and *each site* are genuine, separate OS
  processes (own PID, own TCP port) — that's what the architecture
  diagram and node table are really about. Within a site, the worker
  pool is a fixed set of Python threads sharing the stride-scheduler
  state, rather than one OS process per worker. This keeps the
  scheduler's shared state (pass values, tickets) simple and
  lock-based instead of needing IPC, while still respecting the fixed
  worker-pool-size design ("Bounded overhead; allocation enforced by
  the scheduler, not by process count" — §7). If you need literal
  worker OS processes for the run report, swapping
  `threading.Thread` for `multiprocessing.Process` in
  `Site._worker_loop` and passing shared state through a
  `multiprocessing.Manager` is the natural extension point.
- **"Dominant share" is defined explicitly** (the doc names the
  concept for `least_loaded`/`best_fit` but doesn't give a formula):
  `dominant_share_after` = the *maximum*, across cpu/mem/bw, of
  allocated/capacity after hypothetically placing the request (the
  classic DRF bottleneck-resource definition). `dominant_free_share_after`
  = the *minimum*, across cpu/mem/bw, of (capacity−allocated)/capacity
  after placement — i.e. how much headroom is left on the scarcest
  resource. `least_loaded` picks the site minimising the former;
  `best_fit` picks the site minimising the latter (tightest fit,
  leaves bigger holes elsewhere). See `common/util.py`.
- **A "job"** has no real payload in this project (there's no actual
  traffic to process), so each dispatched job simulates a short unit
  of work: duration drawn from an exponential distribution around
  `job_service_ms` (config, default 6 ms), capped at 5× the mean to
  avoid a long tail. This is what makes slot utilisation and
  jobs/s-per-core meaningful numbers instead of always being 0 or 1.
- **Ingress sites are the edges.** The generator picks each request's
  ingress uniformly from `workload.ingress_sites` (edge-A, edge-B by
  default) — matching the telecom scenario where traffic enters via
  the RAN at the edge, not at the core.
- **Blocking reason when several sites fail for different reasons:**
  the manager reports whichever reason was most common across the
  sites it checked, tie-broken `latency > node_capacity >
  link_bandwidth > unreachable` (`Manager._pick_reason`).
- **Resize target selection**: the generator keeps a rolling window of
  the last 50 accepted `sid`s and, with probability `resize_prob`,
  fires a `RESIZE` against a uniformly random one of them after a
  random delay. A `RESIZE` against an already-terminated instance
  simply comes back `RESIZE_FAIL unknown_or_terminated`, which is
  logged, not treated as an error. **M3 change**: a `RESIZE` that
  doesn't fit locally now tries to migrate the instance to a target
  site with the headroom (`Manager._pick_migration_target`) before
  giving up — see `MILESTONES_3_4.md` for the demo.

## Sample comparison (this implementation, seed 42, default workload)

```
python3 run_demo.py --policy edge_first   --seed 42
python3 run_demo.py --policy least_loaded --seed 42
```

| Metric | edge_first | least_loaded |
|---|---|---|
| Overall blocking probability | 22.0% | 12.7% |
| vRAN-DU blocking probability | 90.0% | 33.3% |

The exact numbers won't match design-doc §9's own reference run
byte-for-byte (a different implementation consumes its seeded RNG in
a different order even with the same seed), but the *direction* is
the same and for the same reason described there: `edge_first` lets
latency-tolerant services (IMS, IoT) fill the scarce edge sites first,
crowding out the DU, which — per `test_du_never_reaches_core_within_2ms`
— has nowhere else it can legally run. `least_loaded` spreads the
tolerant services out, leaving the DU more edge headroom. Re-run with
`--seed` a few different values before treating any single run's
numbers as more than illustrative — the doc makes the same caveat.

## What M3 and M4 closed

The M1 README's "Known limitations" section explicitly enumerated what
was NOT yet implemented. M3 and M4 close those limitations:

| M1 limitation | Closed by | How to demonstrate |
|---|---|---|
| Single point of failure: admission stops if the manager process dies | M4 — WAL + snapshot + `--recover` | kill the manager mid-run, restart with `--recover`, watch it rebuild state |
| Serialised admission: one site round trip at a time | M3 — concurrent admission | decision time drops ~2× (see `MILESTONES_3_4.md`) |
| Failure detection only — instances not recovered or migrated | M3 — SUSPECT evacuation (recovery path) | kill a site mid-run, watch its instances evacuate to healthy sites via `telerm_cli.py status --watch` |
| No migration — a `RESIZE` that doesn't fit the current site simply fails | M3 — resize-driven offload | issue a `RESIZE` that doesn't fit, watch the instance migrate to a site with headroom |
| One machine shares physical cores | out of scope (documented limitation of the demo environment) | n/a |

See `MILESTONES_3_4.md` for the full change-by-change mapping and a
scripted list of presentation-ready commands.
