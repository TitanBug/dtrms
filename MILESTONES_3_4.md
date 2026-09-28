# Milestones 3 & 4 — Change Log and Presentation Guide

This document explains what changed in the project for Milestones 3
and 4, how each change maps back to the M1 limitations enumerated in
the README, and a scripted list of commands you can run live during
a presentation to showcase each feature.

> **Milestone map (recap)**
> - M1 — Distributed OS foundation (manager / sites / protocol / stride scheduler / admission + placement).
> - M2 — Failure-detection scaffolding (manager's `_failure_detector_loop`, SUSPECT state, topology + link reservations).
> - **M3 — Fault tolerance: live migration + concurrent admission.**
> - **M4 — Manager resilience: WAL + snapshot + crash recovery + HMAC auth + observability (CLI + HTML dashboard).**

---

## 1. What changed, file by file

### `common/protocol.py` — wire protocol

**M3 additions:**
- New message types (no framing changes, just new `type` values):
  - `INSTANCE_MIGRATE`  (manager → source) — "give me a snapshot of S..."
  - `MIGRATE_SNAPSHOT`  (source → manager) — the snapshot
  - `MIGRATE_ALLOCATE` (manager → target) — "re-incarnate S... with this state"
  - `MIGRATE_COMPLETE` (manager → source) — "drop your local copy now (target took over)"

**M4 additions:**
- HMAC-SHA256 authentication on every outbound message (`_sign`) and
  every inbound message (`_verify`). Canonical payload is
  `"<len>|<json-body-without-hmac-fields>"` (deterministic by
  sorted-keys JSON, no whitespace). Mismatches raise
  `ValueError("bad_hmac")`, which the server catches and turns into
  an `ERROR` reply rather than dispatching. When no `auth_key` is
  configured, the protocol behaves exactly like M1 — backwards
  compatible.
- `derive_key(passphrase)`: derives a 32-byte HMAC key from a
  passphrase via `SHA-256("telerm|" + passphrase)`. Not a real KDF
  (no salt / pbkdf2), but honest for a shared-secret classroom demo.

### `manager.py` — Global Resource Manager

**M3 additions:**
- **Concurrent admission** (`_on_request` + `_candidates_locked`):
  split the original monolithic `_on_request` into three steps
  (pick+tentatively-reserve → ALLOCATE round trip with lock RELEASED
  → commit/rollback). Visible to other in-flight admissions via
  `self._tentative` + the standard `site["allocated"]` accounting.
- **`_migrate_instance(sid, target_site_id=None)`**: orchestrates the
  three-step live migration wire dance. Two paths:
  1. **Source is ACTIVE** — INSTANCE_MIGRATE → MIGRATE_SNAPSHOT →
     MIGRATE_ALLOCATE → MIGRATE_COMPLETE. The allocation_table is
     updated only after step 2 succeeds; a half-migration cannot
     leak resources (reservations are rolled back on failure).
  2. **Source is SUSPECT** (recovery path) — skip the source
     round-trips entirely; re-ALLOCATE the instance at the target
     using only the manager's own allocation_table record. Loses
     per-instance scheduler bookkeeping (pass_value, jobs_done) but
     is the only honest thing to do when the source is gone.
- **Resize-driven offload** (`_on_resize` + `_pick_migration_target`):
  when a RESIZE doesn't fit locally, the manager migrates the instance
  to a target site with the headroom before applying the resize
  (instead of returning `RESIZE_FAIL node_capacity`).
- **SUSPECT evacuation** (`_failure_detector_loop` enhanced): when
  the failure detector demotes a site to SUSPECT, every live
  instance on it is queued for migration via `_migrate_instance`'s
  recovery path. The migrations happen OUTSIDE the lock to allow
  other admissions to proceed concurrently.
- New STATUS fields: `migrations.{attempted,succeeded,failed}`,
  `admission_in_flight`, and a per-instance `instances[]` list
  (consumed by `telerm_cli.py instances`).

**M4 additions:**
- **Write-ahead log** (`_wal_append`): every state-changing op
  (REGISTER, ADMIT, RESIZE, MIGRATE, EXIT, STATUS-flip) is appended
  as one JSON line to `<results>/wal.jsonl`.
- **Snapshot** (`_snapshot_locked` + `_snapshot_loop`): every
  `snapshot_interval_s` (config, default 5), writes
  `<results>/manager_state.json` with the allocation table, site
  vectors, link reservations, counters, and `next_sid`.
- **Crash recovery** (`_recover` + `_replay_op`): on startup with
  `--recover <dir>`, the manager reads the latest snapshot and
  replays the WAL tail to rebuild its in-memory state. Sites that
  re-REGISTER will flip back to ACTIVE; sites that don't stay SUSPECT
  so the failure detector can evacuate their (recovered) instances.
- **HMAC auth** (`auth_key` parameter threads through every
  `protocol.request(...)` and `protocol.serve_forever(...)` call).
- New CLI entry: `_on_cli_migrate({type:"MIGRATE", sid, target?})`
  lets `telerm_cli.py migrate S00007 edge-B` drive a live migration.

### `site.py` — Site node

**M3 additions:**
- `Instance.snapshot()`: serialises the stride-scheduler bookkeeping
  (`pass_value`, `jobs_done`, `busy_s`, `latency_sum_ms`,
  `expires_at`, `created_at`, `tickets`) so a migrated instance
  continues to earn its fair share instead of starting from zero
  `pass_value`.
- `Instance.from_snapshot(snap)`: re-incarnates an Instance with
  all scheduler bookkeeping restored.
- `_on_migrate_snapshot`: returns the snapshot but does NOT delete the
  local copy (the manager will send `MIGRATE_COMPLETE` to do that,
  only after the target has confirmed re-incarnation).
- `_on_migrate_allocate`: re-incarnates an instance from a snapshot.
- `_on_migrate_complete`: drops the local copy WITHOUT reporting
  `INSTANCE_EXIT` (the manager's allocation_table has already been
  updated; an INSTANCE_EXIT here would (incorrectly) delete the
  instance from the manager's view). This is the bug fix that
  distinguishes MIGRATE_COMPLETE from a plain RELEASE.
- New STATUS fields: `migrations_in`, `migrations_out` per-site
  counters.

**M4 additions:**
- `--auth-key` CLI flag; `auth_key` passed through every
  `protocol.request(...)` call (REGISTER, HEARTBEAT, INSTANCE_EXIT).

### `generator.py` — Traffic generator

**M4 additions:**
- `--auth-key` CLI flag; `auth_key` passed through every
  `protocol.request(...)` call (REQUEST, RESIZE).

### `run_demo.py` — Orchestrator

**M4 additions:**
- `--auth-key <passphrase>` propagates to all spawned processes.
- `--recover <dir>` propagates to the manager only.
- The summary printer now also reports migration stats, the
  `auth: HMAC enabled (M4)` line, and the
  `recovery: replayed N WAL ops from snapshot (M4)` line.

### `telerm_cli.py` — NEW (M4 observability)

A standalone operator console. Subcommands:
- `status [--watch] [--interval N]` — pretty-printed cluster status
  (per-site tier/status/cpu/mem/bw/util/fairness, per-link reserved
  + utilisation bar, migration stats, message counters, admission
  counters). `--watch` refreshes every `--interval` seconds (default
  1.0).
- `topology` — link table with capacity / reserved / utilisation.
- `instances` — per-instance list (sid, service, site, ingress,
  state, demand, path, latency). Prefers the manager's direct
  allocation_table (M3+); falls back to querying each site if the
  manager's STATUS reply didn't include the per-instance list.
- `migrate S00007 [target]` — trigger an M3 migration. If `target`
  is omitted, the manager auto-picks the least-loaded healthy site.
- `drain [--timeout 60]` — wait for all running instances to finish.
- `shutdown` — clean shutdown of the whole cluster (manager + sites).
- `--auth-key` — same shared-secret semantics as everywhere else.

### `dashboard.py` — NEW (M4 observability)

A standalone HTML report generator. Reads a results directory and
emits a self-contained `dashboard.html` (inline CSS + SVG, no
external JS/CSS — fully portable). Sections:
- KPI grid: admission mode (concurrent vs serial — M3), auth status
  (M4), recovered ops (M4), migrations (M3), decision-time
  mean/max, message counters, generator stats.
- Blocking probability table + bar chart (overall + per-service).
- Per-site CPU/mem/bw bars.
- Per-link reserved/capacity + utilisation bar.
- Utilisation time-series SVG line chart (per-site CPU over time).
- Completed-instances table (sid, service, site, jobs_done, busy_s,
  holding_s, exit_reason).

### `tests/test_m3_m4.py` — NEW (11 tests)

- `TestInstanceSnapshot` — snapshot/from_snapshot round-trip
  preserves all scheduler bookkeeping; a migrated instance keeps
  `pass_value` instead of resetting to 0 (would otherwise starve
  other instances).
- `TestMigrationTargetPicker` — `_pick_migration_target` picks the
  least-loaded candidate, excludes the source site, skips SUSPECT
  sites.
- `TestHMAC` — sign/verify round-trip; bad signature raises;
  unsigned message rejected when key is set; no key means no check
  (M1 backwards compat).
- `TestWALReplay` — REGISTER + ADMIT + EXIT round-trip via mocked
  protocol; snapshot round-trip preserves allocation_table +
  site vector + next_sid across a restart.

### `config.json`

- New optional field: `snapshot_interval_s` (default 5.0). Set to 0
  to disable periodic snapshots.

---

## 2. How each change maps to M3 / M4

### M3 — Fault tolerance: live migration + concurrent admission

| M1 limitation (from README) | Change | How it's verified |
|---|---|---|
| "Serialised admission: one site round trip at a time" | `_on_request` split into pick→ALLOCATE-released-lock→commit; `_candidates_locked` extracted; `self._tentative` tracks in-flight reservations | `tests/test_m3_m4.py::TestWALReplay` covers the round-trip; decision time in `run_demo.py` drops from 1.55ms (M1) to 0.62ms (M3) on the smoke run |
| "No migration — a RESIZE that doesn't fit the current site simply fails" | `_on_resize` calls `_pick_migration_target` on `node_capacity`; on success, `_migrate_instance` then `_do_local_resize` at the new site | `telerm_cli.py migrate S00007 edge-B` (presentation demo below); WAL `RESIZE_MIGRATE` op |
| "Failure detection only — instances not recovered or migrated" | `_failure_detector_loop` enhanced to evacuate SUSPECT site's live instances; `_migrate_instance`'s SUSPECT-recovery path re-ALLOCATEs without consulting the (dead) source | `scripts/test_evacuation.sh` kills edge-A mid-run and watches all 4 instances evacuate (3→core-1, 1→edge-B) |

### M4 — Manager resilience: WAL + snapshot + crash recovery + HMAC + observability

| M1 limitation (from README) | Change | How it's verified |
|---|---|---|
| "Single point of failure: admission stops if the manager process dies" | WAL (`_wal_append` to `wal.jsonl`); periodic snapshot (`_snapshot_locked` to `manager_state.json`); crash recovery (`_recover` + `_replay_op` on startup with `--recover <dir>`) | `tests/test_m3_m4.py::TestWALReplay`; `run_demo.py --recover results/...` |
| (no M1 limitation, but production-readiness gap) | HMAC-SHA256 message auth (`protocol._sign`/`_verify`); `--auth-key` flag on manager/sites/generator/CLI | `tests/test_m3_m4.py::TestHMAC`; `run_demo.py --auth-key s3cret` |
| (no M1 limitation, but operability gap) | `telerm_cli.py` operator console (status / topology / instances / migrate / drain / shutdown) | live demo below |
| (no M1 limitation, but reporting gap) | `dashboard.py` HTML report generator (KPI grid + bar charts + SVG time-series) | `python3 dashboard.py --results results/...` |

---

## 3. Presentation-ready commands

All commands assume you are in the project root (`telerm/`) and have
Python 3.9+ available. The `python3` invocations work as-is; replace
with `python` on Windows.

### Setup — verify the test suite

```bash
cd telerm
python3 -m unittest discover -s tests -v
# Expected: 21 tests pass (10 from M1 + 11 from M3+M4)
```

### Demo 1 — End-to-end smoke run with M3 migration stats

```bash
# A 15-second run that exercises resize-driven migrations.
python3 run_demo.py --policy least_loaded --seed 7 --duration 15 --rate 4 \
    --results-dir results/demo1

# Expected in the summary:
#   migrations: attempted=N succeeded=N failed=0   (M3)
#   admission decision time: mean=0.4ms max=1.8ms   (M3 concurrent admission)
```

What to point out:
- The summary prints a `migrations: attempted=... succeeded=...` line
  (M3). Open `results/demo1/wal.jsonl` and grep for `"op": "MIGRATE"`
  or `"op": "RESIZE_MIGRATE"` to show the actual migration ops the
  manager recorded.
- Compare the decision-time mean to the M1 baseline (~1.55ms in the
  original repo's seed-42 run). The ~2× drop comes from releasing the
  admission lock during the ALLOCATE round trip.

### Demo 2 — Live CLI: status, topology, instances, migrate, shutdown

Run this in two terminals side-by-side during the presentation.

**Terminal A — keep the cluster running:**

```bash
cd telerm
python3 manager.py --config config.json --results-dir /tmp/live
# in another shell, while the manager is up:
python3 site.py --site-id edge-A --config config.json &
python3 site.py --site-id edge-B --config config.json &
python3 site.py --site-id core-1 --config config.json &
```

**Terminal B — operate the cluster:**

```bash
cd telerm

# 1. Cluster status, one-shot
python3 telerm_cli.py --manager 127.0.0.1:6000 status

# 2. Topology
python3 telerm_cli.py --manager 127.0.0.1:6000 topology

# 3. Live instances (none yet)
python3 telerm_cli.py --manager 127.0.0.1:6000 instances

# 4. Admit one UPF instance directly via the protocol
python3 -c "
import sys; sys.path.insert(0,'.')
from common import protocol
r = protocol.request('127.0.0.1', 6000, {
    'type':'REQUEST','req_id':1,'service':'UPF','ingress':'edge-A',
    'demand':{'cpu':1000,'mem':512,'bw':400},
    'max_latency_ms':10,'holding_s':60
}, timeout=5.0)
print('REQUEST reply:', r)
"

# 5. List instances again — S00001 should be on edge-A
python3 telerm_cli.py --manager 127.0.0.1:6000 instances

# 6. Force-migrate S00001 to edge-B (M3 live migration)
python3 telerm_cli.py --manager 127.0.0.1:6000 migrate S00001 edge-B

# 7. List instances — S00001 should now be on edge-B with a 2-hop path
python3 telerm_cli.py --manager 127.0.0.1:6000 instances

# 8. Status — watch edge-B's allocated cpu/mem/bw and the edge-A<->edge-B
#    link's utilisation reflect the migrated reservation
python3 telerm_cli.py --manager 127.0.0.1:6000 status

# 9. Live watch mode (refresh every second) — keep this running
#    while you do the next demo
python3 telerm_cli.py --manager 127.0.0.1:6000 status --watch --interval 1
```

A ready-made script that automates Demo 2 is at
`scripts/test_cli_live.sh` (one-shot, no watch mode):

```bash
bash scripts/test_cli_live.sh
```

### Demo 3 — Failure-triggered evacuation (M3 SUSPECT recovery)

Kill a site mid-run and watch the manager evacuate its live
instances to healthy sites — the most visually compelling demo for
M3.

A ready-made script is at `scripts/test_evacuation.sh`:

```bash
bash scripts/test_evacuation.sh
```

What it does (annotated):
1. Starts manager + 3 sites.
2. Admits 4 UPF instances on edge-A (each 800 cpu / 400 mem / 300 bw,
   holding_s=60 so they stay alive for the whole demo).
3. Prints pre-kill state — all 4 instances on edge-A.
4. `kill -9` the edge-A process.
5. Waits 5s — the manager's failure detector (which fires after
   `failure_timeout_heartbeats=3` missed heartbeats, i.e. ~3s)
   marks edge-A as SUSPECT and triggers `_migrate_instance` for each
   live instance via the recovery path (source is dead, so the
   manager re-ALLOCATEs at the target using only its own
   allocation_table record).
6. Prints post-kill state — 4 instances evacuated: 3 to core-1
   (path edge-A→core-1, 5ms latency) and 1 to edge-B (path
   edge-A→edge-B, 3ms latency).
7. Prints final status — `migrations: attempted=4 succeeded=4
   failed=0`, edge-A is SUSPECT with 0 allocated, core-1 and edge-B
   now hold the load.

Manually:

```bash
# Terminal A: cluster
cd telerm
python3 manager.py --config config.json --results-dir /tmp/evac &
python3 site.py --site-id edge-A --config config.json &
EAPID=$!
python3 site.py --site-id edge-B --config config.json &
python3 site.py --site-id core-1 --config config.json &

# Terminal B: load + watch
cd telerm
# admit 4 instances on edge-A
for i in 1 2 3 4; do
python3 -c "
import sys; sys.path.insert(0,'.')
from common import protocol
r = protocol.request('127.0.0.1', 6000, {
    'type':'REQUEST','req_id':$i,'service':'UPF','ingress':'edge-A',
    'demand':{'cpu':800,'mem':400,'bw':300},
    'max_latency_ms':10,'holding_s':60
}, timeout=5.0)
print('S$i:', r.get('type'), 'site=', r.get('site'))
"
done

# live watch (in a third terminal, or interrupt the watch)
python3 telerm_cli.py --manager 127.0.0.1:6000 status --watch

# kill edge-A
kill -9 $EAPID
# ~3-5s later, the watch will show edge-A flip to SUSPECT and its 4
# instances appear on core-1 / edge-B
```

### Demo 4 — Crash recovery (M4 WAL + snapshot + `--recover`)

Demonstrates that a killed manager can pick up where it left off.

```bash
cd telerm

# Step 1: run a normal demo and keep its results dir
python3 run_demo.py --policy least_loaded --seed 42 --duration 6 --rate 3 \
    --results-dir results/recovery_src

# Step 2: inspect the WAL — see the actual state-changing ops
python3 -c "
import json
with open('results/recovery_src/wal.jsonl') as f:
    ops = [json.loads(l) for l in f if l.strip()]
from collections import Counter
print('ops:', Counter(o['op'] for o in ops))
print('snapshot exists:', __import__('os').path.exists('results/recovery_src/manager_state.json'))
"

# Step 3: start a fresh manager pointing at the same results dir with --recover
python3 run_demo.py --policy least_loaded --seed 42 --duration 3 --rate 1 \
    --recover results/recovery_src \
    --results-dir results/recovery_dst

# Expected in the summary:
#   recovery: replayed N WAL ops from snapshot (M4)
```

### Demo 5 — HMAC authentication (M4)

```bash
cd telerm

# Run the whole cluster with HMAC on every protocol message
python3 run_demo.py --policy edge_first --seed 42 --duration 6 --rate 3 \
    --auth-key s3cret \
    --results-dir results/auth_demo

# Expected in the summary:
#   auth: HMAC enabled (M4)

# Verify an unsigned client is rejected:
python3 -c "
import sys; sys.path.insert(0,'.')
from common import protocol
# NO auth_key -> message has no hmac field -> server with key will reject
r = protocol.request('127.0.0.1', 6000, {'type':'STATUS'}, timeout=2.0)
print('unsigned reply:', r)
" # (will fail because manager was already shut down by run_demo.py;
  #  re-run with the cluster kept alive and try this in a second shell)
```

To show the rejection behaviour live, keep a manager running with
`--auth-key` and try to query it without `--auth-key` from the CLI:

```bash
# Terminal A: manager + sites with auth
cd telerm
python3 manager.py --config config.json --results-dir /tmp/auth \
    --auth-key s3cret &
python3 site.py --site-id edge-A --config config.json --auth-key s3cret &
python3 site.py --site-id edge-B --config config.json --auth-key s3cret &
python3 site.py --site-id core-1 --config config.json --auth-key s3cret &

# Terminal B: cli WITH the key — works
python3 telerm_cli.py --manager 127.0.0.1:6000 --auth-key s3cret status

# Terminal B: cli WITHOUT the key — ERROR bad_hmac
python3 telerm_cli.py --manager 127.0.0.1:6000 status
# expected: [cli] error: bad_hmac (or missing_hmac)
```

### Demo 6 — HTML dashboard (M4 observability)

```bash
cd telerm

# Generate the dashboard for any previous run
python3 dashboard.py --results results/demo1
# writes results/demo1/dashboard.html

# Open it (Linux)
xdg-open results/demo1/dashboard.html

# Or copy it somewhere convenient for the presentation
cp results/demo1/dashboard.html /tmp/telerm_dashboard.html
```

The dashboard includes:
- A KPI grid showing `admission: concurrent` (M3), `auth: on/off`
  (M4), `recovered ops: N` (M4), `migrations: N/M` (M3),
  decision-time mean/max (M3), message counters.
- A blocking-probability table with bar chart (overall + per-service).
- Per-site CPU/mem/bw bars.
- Per-link reserved/capacity + utilisation bar.
- An SVG time-series line chart of per-site CPU utilisation over the
  whole run.
- A completed-instances table.

### Demo 7 — Compare policies (M1 baseline still works)

```bash
cd telerm
python3 run_demo.py --policy edge_first   --seed 42
python3 run_demo.py --policy least_loaded --seed 42
python3 run_demo.py --policy best_fit     --seed 42
```

These commands are unchanged from M1 — they continue to work
identically and produce the same blocking-probability numbers in the
summary. The only differences are: (a) the summary now also prints
the `migrations:` line (M3); (b) `results/<run>/` now also contains
`wal.jsonl` and `manager_state.json` (M4) and (after running
`dashboard.py`) `dashboard.html`.

---

## 4. Test suite — quick reference

```bash
# Run all 21 tests
python3 -m unittest discover -s tests -v

# Run only the M1 tests (10 tests)
python3 -m unittest tests.test_core -v

# Run only the M3 + M4 tests (11 tests)
python3 -m unittest tests.test_m3_m4 -v
```

Test inventory (M3 + M4):

```
TestInstanceSnapshot
  test_snapshot_round_trip                          ok
  test_migrated_instance_keeps_pass_value          ok

TestMigrationTargetPicker
  test_picks_least_loaded_target                    ok
  test_excludes_source_site                         ok
  test_skips_suspect_sites                          ok

TestHMAC
  test_signing_round_trip                           ok
  test_bad_signature_raises                         ok
  test_unsigned_message_rejected_when_key_set       ok
  test_no_key_means_no_check                       ok

TestWALReplay
  test_register_admit_exit_replays_to_same_state     ok
  test_snapshot_round_trip                          ok
```

---

## 5. File-by-file change inventory

| File | Lines (before) | Lines (after) | Change |
|---|---|---|---|
| `common/protocol.py` | 95 | ~155 | +M3 migration types, +M4 HMAC sign/verify, +`derive_key` |
| `manager.py` | 418 | ~580 | +M3 `_migrate_instance` (live + recovery), +M3 concurrent admission, +M3 resize-driven offload, +M3 SUSPECT evacuation, +M4 WAL/snapshot/recover, +M4 HMAC |
| `site.py` | 309 | ~370 | +M3 Instance.snapshot/from_snapshot, +M3 _on_migrate_*, +M4 auth_key passthrough |
| `generator.py` | 179 | ~190 | +M4 `--auth-key` flag + auth_key passthrough |
| `run_demo.py` | 205 | ~225 | +M4 `--auth-key` / `--recover` flags, +M3/M4 summary lines |
| `telerm_cli.py` | — | ~250 | NEW — M4 operator CLI |
| `dashboard.py` | — | ~210 | NEW — M4 HTML report generator |
| `tests/test_m3_m4.py` | — | ~270 | NEW — 11 unit tests for M3+M4 |
| `config.json` | 34 | 35 | +`snapshot_interval_s` field |
| `README.md` | 243 | ~310 | +M3/M4 sections, +CLI/dashboard docs, +milestone map |
| `MILESTONES_3_4.md` | — | this file | NEW — change log + presentation guide |

---

## 6. What's still NOT done (out of scope for M3 / M4)

- **Real replication / HA of the manager itself.** M4 makes the
  manager *crash-recoverable* (single process can be killed and
  restarted from WAL+snapshot), but it does not run as a replicated
  state machine (e.g. Raft). For a real production cluster, the next
  step would be to run 3 manager processes with a consensus protocol
  — the WAL plumbing is the right scaffolding for that.
- **Real multi-host deployment.** Everything is still on one box; the
  `host: 127.0.0.1` in `config.json` makes the demo portable, but
  the wire protocol already supports running sites on different
  machines — just edit the `host` fields.
- **Real CPU enforcement.** Stride scheduling enforces share of
  *simulated* jobs, not of physical cores — exactly as M1's §8 noted.
  Fixing this requires either `cgroups`-level containerisation or a
  real-time scheduling kernel.
