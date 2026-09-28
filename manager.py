#!/usr/bin/env python3
"""
TeleRM Global Resource Manager.

Original Milestone-1 design (design doc section 3, "Global Resource
Manager" box): Admission control, Placement policy, Allocation table,
Topology + link reservations, Site registry + failure detector,
Utilisation sampler.

Milestone 3 additions
---------------------
1. Live instance migration (`_migrate_instance`). Used in two cases:
   (a) A site's heartbeat times out -> SUSPECT -> evacuate all its
       live instances to other healthy sites before they expire.
   (b) A RESIZE that doesn't fit locally -> try to offload the instance
       to another site that has the headroom (instead of failing).
   The wire dance is INSTANCE_MIGRATE (snapshot at source) ->
   MIGRATE_ALLOCATE (reincarnate at target) -> MIGRATE_COMPLETE
   (release at source). All three round-trips are rolled back on
   failure so a half-migration cannot leak resources.

2. Concurrent admission. The M1 design held `self.lock` for the entire
   ALLOCATE round trip, bounding admission throughput to one site round
   trip at a time. M3 splits that into:
     (a) short critical section: pick candidate, tentatively reserve
     (b) ALLOCATE round trip with the lock RELEASED
     (c) short critical section: commit (on ALLOCATE_OK) or rollback
   A per-instance "tentative" flag in the allocation table makes
   reservations visible to other in-flight admissions so that two
   concurrent admissions don't both pick the only-just-fits site.

Milestone 4 additions
---------------------
3. Write-ahead log (WAL). Every state-changing operation
   (REGISTER, REQUEST accepted, RESIZE ok, INSTANCE_EXIT, MIGRATE,
   status flip) is appended as one JSON line to `wal.jsonl` in the
   results directory. Safe-crash recovery replays it.

4. Snapshot + recovery. Every `snapshot_interval_s` (default 5s), the
   manager writes `manager_state.json` — a compact dump of the
   allocation table, site vector, counters. On startup with
   `--recover <results_dir>`, the manager reads the latest snapshot
   and replays the WAL tail to rebuild its in-memory state. The
   scenario "kill the manager mid-run, restart it, keep serving"
   is demonstrated in `MILESTONES_3_4.md`.

5. HMAC authentication. If `--auth-key <passphrase>` is set, every
   outbound message is signed and every inbound message is verified;
   sites started with the same passphrase will refuse unsigned or
   tampered messages.

Run standalone:
    python3 manager.py --config config.json --results-dir results/run1
"""
import argparse
import json
import os
import sys
import threading
import time
from datetime import datetime

from common import protocol, util
from common.counters import Counters
from common.topology import Topology


class Manager:
    def __init__(self, config: dict, results_dir: str, policy: str = None,
                 auth_key: bytes = None, recover_dir: str = None):
        self.config = config
        self.results_dir = results_dir
        self.policy = policy or config.get("placement_policy", "edge_first")
        self.heartbeat_interval = config.get("heartbeat_interval_s", 1.0)
        self.failure_timeout_hb = config.get("failure_timeout_heartbeats", 3)
        self.fairness_threshold = config.get("fairness_saturation_threshold", 0.9)
        self.snapshot_interval = config.get("snapshot_interval_s", 5.0)
        self.auth_key = auth_key

        os.makedirs(self.results_dir, exist_ok=True)
        self.topology = Topology(config["links"])
        self.counters = Counters()

        # --- protected state ---
        # admission no longer holds this lock for the whole ALLOCATE
        # round trip (M3 concurrent admission); only for the candidate
        # selection + tentative reservation + commit/rollback steps.
        self.lock = threading.RLock()
        self.sites = {}            # site_id -> record
        self.allocation_table = {}  # sid -> instance record
        self._next_sid = 1
        # in-flight admissions track their tentative reservations so
        # a concurrent REQUEST can see them in the candidate scan
        self._tentative = {}        # sid -> {"site", "demand", "path", "ts"}
        self._admission_in_flight = 0

        # metrics
        self.offered = {"_all": 0}
        self.blocked = {"_all": 0}
        self.blocked_reasons = {"_all": {}}
        self.admitted = {"_all": 0}
        self.decision_times_ms = []
        self.completed_instances = []   # finished instance summaries
        self.heartbeat_log = []         # raw heartbeat records
        self.utilisation_samples = []
        self.migrations_attempted = 0
        self.migrations_succeeded = 0
        self.migrations_failed = 0
        self.recover_applied = 0

        self._stop = threading.Event()
        self._srv = None

        # M4: persistence paths
        self._wal_path = os.path.join(self.results_dir, "wal.jsonl")
        self._snap_path = os.path.join(self.results_dir, "manager_state.json")

        if recover_dir:
            self._recover(recover_dir)

    # ---------------------------------------------------------- lifecycle
    def start(self):
        host, port = self.config["manager"]["host"], self.config["manager"]["port"]
        self._srv = protocol.serve_forever(
            host, port, self._dispatch,
            counters=self.counters, auth_key=self.auth_key,
        )
        threading.Thread(target=self._sampler_loop, daemon=True).start()
        threading.Thread(target=self._failure_detector_loop, daemon=True).start()
        if self.snapshot_interval and self.snapshot_interval > 0:
            threading.Thread(target=self._snapshot_loop, daemon=True).start()
        print(f"[manager] listening on {host}:{port} (policy={self.policy})"
              f"{', auth=on' if self.auth_key else ''}"
              f"{', recovered=' + str(self.recover_applied) + ' ops' if self.recover_applied else ''}",
              flush=True)

    def stop(self):
        self._stop.set()
        if self._srv:
            try:
                self._srv.close()
            except OSError:
                pass

    # ---------------------------------------------------------- dispatch
    def _dispatch(self, msg: dict) -> dict:
        t = msg.get("type")
        try:
            if t == "REGISTER":
                return self._on_register(msg)
            if t == "HEARTBEAT":
                return self._on_heartbeat(msg)
            if t == "REQUEST":
                return self._on_request(msg)
            if t == "RESIZE":
                return self._on_resize(msg)
            if t == "INSTANCE_EXIT":
                return self._on_instance_exit(msg)
            if t == "MIGRATE":            # CLI-driven migration (M3 + CLI)
                return self._on_cli_migrate(msg)
            if t == "STATUS":
                return self._status()
            if t == "SHUTDOWN":
                threading.Thread(target=self._delayed_shutdown, daemon=True).start()
                return {"type": "SHUTDOWN_OK"}
        except Exception as e:  # pragma: no cover - defensive
            return {"type": "ERROR", "error": str(e)}
        return {"type": "ERROR", "error": f"unknown message type {t!r}"}

    def _delayed_shutdown(self):
        time.sleep(0.2)  # let the SHUTDOWN_OK reply flush first
        self._snapshot_locked("shutdown")
        self._write_report()
        self.stop()

    # ---------------------------------------------------------- REGISTER
    def _on_register(self, msg):
        with self.lock:
            sid = msg["site_id"]
            self.sites[sid] = {
                "site_id": sid,
                "host": msg["host"],
                "port": msg["port"],
                "tier": msg["tier"],
                "capacity": dict(msg["capacity"]),
                "allocated": {"cpu": 0, "mem": 0, "bw": 0},
                "workers": msg.get("workers", 1),
                "status": "ACTIVE",
                "last_heartbeat": time.time(),
                "last_fairness": None,
                "last_slot_util": None,
            }
            self._wal_append({"op": "REGISTER", "site_id": sid, "tier": msg["tier"],
                              "host": msg["host"], "port": msg["port"],
                              "capacity": dict(msg["capacity"])})
        print(f"[manager] REGISTER {sid} ({msg['tier']}) at {msg['host']}:{msg['port']}", flush=True)
        return {"type": "REGISTER_OK", "site_id": sid}

    # --------------------------------------------------------- HEARTBEAT
    def _on_heartbeat(self, msg):
        with self.lock:
            sid = msg["site_id"]
            site = self.sites.get(sid)
            if site is None:
                return {"type": "ERROR", "error": "unregistered site"}
            site["last_heartbeat"] = time.time()
            if site["status"] == "SUSPECT":
                print(f"[manager] {sid} back ACTIVE", flush=True)
                self._wal_append({"op": "STATUS", "site_id": sid, "status": "ACTIVE"})
            site["status"] = "ACTIVE"
            site["last_fairness"] = msg.get("fairness_j")
            site["last_slot_util"] = msg.get("slot_utilisation")
            self.heartbeat_log.append({
                "ts": msg.get("ts", time.time()),
                "site_id": sid,
                "slot_utilisation": msg.get("slot_utilisation"),
                "jobs_interval": msg.get("jobs_interval"),
                "fairness_j": msg.get("fairness_j"),
                "instances_running": msg.get("instances_running"),
            })
        return {"type": "HEARTBEAT_OK"}

    # ----------------------------------------------------------- REQUEST
    def _on_request(self, msg):
        """M3: concurrent admission. Three steps:
          (a) short critical section: bump offered counter, scan
              candidates, pick the winner, tentatively reserve the
              node vector + path bandwidth at the chosen site.
          (b) ALLOCATE round trip with the lock RELEASED — other
              admissions can proceed concurrently and will see our
              tentative reservation in the candidate scan.
          (c) short critical section: commit (on ALLOCATE_OK) or
              roll back (on any failure)."""
        t0 = time.perf_counter()
        service = msg["service"]
        ingress = msg["ingress"]
        demand = msg["demand"]
        max_latency = msg["max_latency_ms"]
        holding_s = msg["holding_s"]

        # ---- (a) pick + tentatively reserve, under self.lock ----------
        with self.lock:
            self.offered["_all"] += 1
            self.offered[service] = self.offered.get(service, 0) + 1

            candidates, fail_reasons = self._candidates_locked(ingress, demand, max_latency)
            if not candidates:
                reason = self._pick_reason(fail_reasons)
                self._record_blocked(service, reason)
                dt = (time.perf_counter() - t0) * 1000
                self.decision_times_ms.append(dt)
                return {"type": "BLOCKED", "req_id": msg.get("req_id"),
                        "reason": reason, "decision_ms": round(dt, 3)}

            chosen = self._apply_policy(candidates, ingress)
            sid = chosen["site_id"]
            site = self.sites[sid]
            path = chosen["path"]

            # tentative reservation visible to other concurrent admissions
            for r in site["capacity"]:
                site["allocated"][r] += demand.get(r, 0)
            self.topology.reserve_path(path, demand["bw"])
            new_sid = util.format_sid(self._next_sid)
            self._next_sid += 1
            self._tentative[new_sid] = {
                "site": sid, "demand": dict(demand), "path": path,
                "ts": time.time(),
            }
            self._admission_in_flight += 1

        # ---- (b) ALLOCATE round trip, lock RELEASED -------------------
        try:
            reply = protocol.request(
                site["host"], site["port"],
                {"type": "ALLOCATE", "sid": new_sid, "service": service,
                 "demand": demand, "holding_s": holding_s},
                counters=self.counters, auth_key=self.auth_key,
            )
            ok = reply.get("type") == "ALLOCATE_OK"
        except (ConnectionError, OSError, TimeoutError):
            ok = False

        # ---- (c) commit or roll back under self.lock -----------------
        with self.lock:
            self._admission_in_flight -= 1
            self._tentative.pop(new_sid, None)
            if not ok:
                # roll back the tentative reservation (M1 §5.1 + M3)
                for r in site["capacity"]:
                    site["allocated"][r] -= demand.get(r, 0)
                self.topology.release_path(path, demand["bw"])
                self._record_blocked(service, "site_unreachable")
                dt = (time.perf_counter() - t0) * 1000
                self.decision_times_ms.append(dt)
                return {"type": "BLOCKED", "req_id": msg.get("req_id"),
                        "reason": "site_unreachable", "decision_ms": round(dt, 3)}

            self.allocation_table[new_sid] = {
                "sid": new_sid, "service": service, "site": sid, "ingress": ingress,
                "demand": dict(demand), "path": path,
                "path_latency_ms": chosen["path_latency_ms"],
                "holding_s": holding_s, "admitted_at": time.time(),
                "state": "RUNNING",
            }
            self.admitted["_all"] += 1
            self.admitted[service] = self.admitted.get(service, 0) + 1
            self._wal_append({"op": "ADMIT", "sid": new_sid, "service": service,
                              "site": sid, "ingress": ingress,
                              "demand": dict(demand), "path": path,
                              "path_latency_ms": chosen["path_latency_ms"],
                              "holding_s": holding_s,
                              "admitted_at": self.allocation_table[new_sid]["admitted_at"]})

            dt = (time.perf_counter() - t0) * 1000
            self.decision_times_ms.append(dt)
            return {
                "type": "ACCEPTED", "req_id": msg.get("req_id"), "sid": new_sid, "site": sid,
                "path": path, "path_latency_ms": chosen["path_latency_ms"],
                "decision_ms": round(dt, 3),
            }

    def _candidates_locked(self, ingress, demand, max_latency):
        """Scan all ACTIVE sites for admission candidates. Called with
        self.lock held. Visible tentative reservations (in
        self._tentative) are NOT subtracted separately because their
        demand is already counted in site['allocated'] -- the
        concurrent admission made its reservation under the same lock."""
        candidates = []
        fail_reasons = []
        for sid, site in self.sites.items():
            if site["status"] != "ACTIVE":
                continue
            path, lat = self.topology.shortest_path(ingress, sid)
            if path is None:
                fail_reasons.append("unreachable")
                continue
            if lat > max_latency:
                fail_reasons.append("latency")
                continue
            if not util.fits(site["allocated"], demand, site["capacity"]):
                fail_reasons.append("node_capacity")
                continue
            bw_ok, _blocking_link = self.topology.path_has_bandwidth(path, demand["bw"])
            if not bw_ok:
                fail_reasons.append("link_bandwidth")
                continue
            allocated_after = {r: site["allocated"][r] + demand.get(r, 0) for r in site["capacity"]}
            candidates.append({
                "site_id": sid,
                "path": path,
                "path_latency_ms": lat,
                "dominant_share_after": util.dominant_share(allocated_after, site["capacity"]),
                "dominant_free_share_after": util.dominant_free_share(allocated_after, site["capacity"]),
            })
        return candidates, fail_reasons

    def _record_blocked(self, service, reason):
        self.blocked["_all"] += 1
        self.blocked[service] = self.blocked.get(service, 0) + 1
        sr = self.blocked_reasons.setdefault(service, {})
        sr[reason] = sr.get(reason, 0) + 1
        self.blocked_reasons["_all"][reason] = self.blocked_reasons["_all"].get(reason, 0) + 1

    @staticmethod
    def _pick_reason(reasons):
        if not reasons:
            return "no_active_sites"
        priority = ["latency", "node_capacity", "link_bandwidth", "unreachable"]
        counts = {}
        for r in reasons:
            counts[r] = counts.get(r, 0) + 1
        best = max(counts.items(), key=lambda kv: (kv[1], -priority.index(kv[0]) if kv[0] in priority else -99))
        return best[0]

    def _apply_policy(self, candidates, ingress):
        if self.policy == "least_loaded":
            key = lambda c: c["dominant_share_after"]
        elif self.policy == "best_fit":
            key = lambda c: c["dominant_free_share_after"]
        else:  # edge_first (default): ingress falls out of "lowest latency"
               # first, since only the ingress site has a zero-latency path;
               # ties broken by least loaded.
            key = lambda c: (c["path_latency_ms"], c["dominant_share_after"])
        return min(candidates, key=key)

    # ------------------------------------------------------------ RESIZE
    def _on_resize(self, msg):
        """M3: on a resize that doesn't fit locally, try to migrate the
        instance to another site that has the headroom before returning
        RESIZE_FAIL. This directly closes the M1 limitation 'no
        migration -- a RESIZE that doesn't fit the current site simply
        fails'."""
        with self.lock:
            sid = msg["sid"]
            inst = self.allocation_table.get(sid)
            if inst is None or inst["state"] != "RUNNING":
                return {"type": "RESIZE_FAIL", "req_id": msg.get("req_id"),
                        "reason": "unknown_or_terminated"}
            factor = msg["factor"]
            old_cpu = inst["demand"]["cpu"]
            new_cpu = max(1, round(old_cpu * factor))
            extra = new_cpu - old_cpu
            cur_site = self.sites[inst["site"]]

            # case 1: shrinks or fits locally -> same as M1
            if extra <= 0 or cur_site["allocated"]["cpu"] + extra <= cur_site["capacity"]["cpu"] + 1e-9:
                reply = self._do_local_resize(sid, inst, new_cpu, extra, cur_site)
                return reply

            # case 2: doesn't fit locally -> try migrating to a site that does
            # (M3: this used to be a hard RESIZE_FAIL node_capacity)
            release_after = self._tentative_demand(inst)
            target = self._pick_migration_target(inst, release_after, demand_override={"cpu": new_cpu})
            if target is None:
                return {"type": "RESIZE_FAIL", "req_id": msg.get("req_id"),
                        "reason": "node_capacity"}

        # outside the lock: actual migration round trips
        migrated = self._migrate_instance(sid, target_site_id=target)
        if not migrated:
            return {"type": "RESIZE_FAIL", "req_id": msg.get("req_id"),
                    "reason": "migration_failed"}
        # migration succeeded; the instance is now on `target` with the
        # old cpu demand. Apply the resize to the new site (which we
        # know has headroom, since _pick_migration_target checked).
        with self.lock:
            inst = self.allocation_table.get(sid)
            if inst is None or inst["state"] != "RUNNING":
                return {"type": "RESIZE_FAIL", "req_id": msg.get("req_id"),
                        "reason": "lost_in_migration"}
            new_site = self.sites[inst["site"]]
            extra = new_cpu - old_cpu  # recompute (old_cpu unchanged)
            reply = self._do_local_resize(sid, inst, new_cpu, extra, new_site)
            if reply.get("type") == "RESIZE_OK":
                self._wal_append({"op": "RESIZE_MIGRATE", "sid": sid,
                                  "from_site": cur_site["site_id"],
                                  "to_site": new_site["site_id"],
                                  "new_cpu": new_cpu})
            return reply

    def _do_local_resize(self, sid, inst, new_cpu, extra, site):
        try:
            reply = protocol.request(
                site["host"], site["port"],
                {"type": "RESIZE", "sid": sid, "cpu": new_cpu},
                counters=self.counters, auth_key=self.auth_key)
            ok = reply.get("type") == "RESIZE_OK"
        except (ConnectionError, OSError, TimeoutError):
            ok = False
        if not ok:
            return {"type": "RESIZE_FAIL", "req_id": None,
                    "reason": "site_unreachable"}
        site["allocated"]["cpu"] += extra
        inst["demand"]["cpu"] = new_cpu
        self._wal_append({"op": "RESIZE", "sid": sid, "new_cpu": new_cpu,
                          "site": site["site_id"]})
        return {"type": "RESIZE_OK", "req_id": None, "sid": sid, "cpu": new_cpu}

    def _tentative_demand(self, inst):
        """The full per-resource demand of an instance, used when freeing
        its reservation at the current site before migration."""
        return dict(inst["demand"])

    def _pick_migration_target(self, inst, demand_at_target, demand_override=None,
                              exclude=None):
        """Pick a target site for migrating `inst`. The instance's
        post-migration demand is `demand_at_target` overridden by
        `demand_override` (e.g. {"cpu": new_cpu} for a resize-driven
        migration). Returns a site_id or None."""
        effective_demand = dict(demand_at_target)
        if demand_override:
            effective_demand.update(demand_override)
        exclude = exclude or set()
        exclude.add(inst["site"])
        # we want the new site to be reachable from the instance's
        # original ingress (so the path's latency bound still holds),
        # but if no such site exists we relax the latency check and
        # let the caller decide what to do.
        ingress = inst["ingress"]
        max_lat = self._max_latency_for_service(inst["service"])
        candidates = []
        for sid, site in self.sites.items():
            if sid in exclude or site["status"] != "ACTIVE":
                continue
            path, lat = self.topology.shortest_path(ingress, sid)
            if path is None:
                continue
            if lat > max_lat:
                continue
            if not util.fits(site["allocated"], effective_demand, site["capacity"]):
                continue
            bw_ok, _ = self.topology.path_has_bandwidth(path, effective_demand["bw"])
            if not bw_ok:
                continue
            candidates.append((sid, path, lat))
        if not candidates:
            # relax the latency check as a fallback (a slower-path
            # migration is better than losing the instance)
            for sid, site in self.sites.items():
                if sid in exclude or site["status"] != "ACTIVE":
                    continue
                path, lat = self.topology.shortest_path(ingress, sid)
                if path is None:
                    continue
                if not util.fits(site["allocated"], effective_demand, site["capacity"]):
                    continue
                bw_ok, _ = self.topology.path_has_bandwidth(path, effective_demand["bw"])
                if not bw_ok:
                    continue
                candidates.append((sid, path, lat))
        if not candidates:
            return None
        # pick least-loaded target
        def share(item):
            sid, _, _ = item
            site = self.sites[sid]
            alloc_after = {r: site["allocated"][r] + effective_demand.get(r, 0) for r in site["capacity"]}
            return util.dominant_share(alloc_after, site["capacity"])
        sid, path, lat = min(candidates, key=share)
        return sid

    def _max_latency_for_service(self, service):
        spec = self.config.get("services", {}).get(service, {})
        return spec.get("max_latency_ms", 1e9)

    # ------------------------------------------------------- INSTANCE_EXIT
    def _on_instance_exit(self, msg):
        with self.lock:
            sid = msg["sid"]
            inst = self.allocation_table.get(sid)
            if inst is None:
                return {"type": "INSTANCE_EXIT_OK"}
            site = self.sites.get(inst["site"])
            if site is not None:
                for r in site["capacity"]:
                    site["allocated"][r] = max(0, site["allocated"][r] - inst["demand"].get(r, 0))
            self.topology.release_path(inst["path"], inst["demand"]["bw"])
            inst["state"] = "TERMINATED"
            summary = dict(msg.get("summary", {}))
            summary.update({"sid": sid, "service": inst["service"], "site": inst["site"]})
            self.completed_instances.append(summary)
            del self.allocation_table[sid]
            self._wal_append({"op": "EXIT", "sid": sid, "site": inst["site"]})
        return {"type": "INSTANCE_EXIT_OK"}

    # ------------------------------------------------------- M3: MIGRATE
    def _migrate_instance(self, sid, target_site_id=None):
        """Live-migrate instance `sid` to `target_site_id` (auto-pick if
        None). Returns True on success, False on any failure. All
        reservations are rolled back on failure so a half-migration
        cannot leak resources. The three-step wire dance:

            step 1: INSTANCE_MIGRATE  -> source  -> MIGRATE_SNAPSHOT
            step 2: MIGRATE_ALLOCATE   -> target  -> ALLOCATE_OK
            step 3: MIGRATE_COMPLETE   -> source  -> RELEASE_OK
                    (only sent after step 2 succeeds; if step 2 fails,
                    the source keeps the instance alive.)

        The manager's allocation_table is updated only after step 2
        succeeds, so a crash mid-migration either finds the instance
        still at the source (steps 1/2 failed) or at the target (step
        2 ok, step 3 lost -> source still has a stale copy that the
        next INSTANCE_EXIT will naturally reap).

        SUSPECT source recovery: when the source site is SUSPECT (i.e.
        unreachable), we skip the source round-trip entirely and re-
        ALLOCATE the instance at the target using only the manager's
        own allocation_table state. This loses the per-instance
        scheduler bookkeeping (pass_value, jobs_done) since the source
        is gone, but it's the only honest thing to do — we can't
        reach the source to take a snapshot. This closes the M1
        limitation 'instances not recovered or migrated' for the
        failed-site case."""
        with self.lock:
            inst = self.allocation_table.get(sid)
            if inst is None or inst["state"] != "RUNNING":
                self.migrations_failed += 1
                return False
            source_id = inst["site"]
            source = self.sites.get(source_id)
            if source is None:
                self.migrations_failed += 1
                return False

            demand = dict(inst["demand"])
            if target_site_id is None:
                target_site_id = self._pick_migration_target(inst, demand)
            if target_site_id is None or target_site_id == source_id:
                self.migrations_failed += 1
                return False
            target = self.sites.get(target_site_id)
            if target is None or target["status"] != "ACTIVE":
                self.migrations_failed += 1
                return False

            new_path, new_lat = self.topology.shortest_path(inst["ingress"], target_site_id)
            if new_path is None:
                self.migrations_failed += 1
                return False
            if not util.fits(target["allocated"], demand, target["capacity"]):
                self.migrations_failed += 1
                return False
            bw_ok, _ = self.topology.path_has_bandwidth(new_path, demand["bw"])
            if not bw_ok:
                self.migrations_failed += 1
                return False

            # tentatively reserve on the target + new path
            for r in target["capacity"]:
                target["allocated"][r] += demand.get(r, 0)
            self.topology.reserve_path(new_path, demand["bw"])
            inst["state"] = "MIGRATING"
            self.migrations_attempted += 1
            source_is_suspect = (source["status"] == "SUSPECT")

        # SUSPECT source: skip the source round-trips entirely
        if source_is_suspect:
            # re-ALLOCATE at the target using only the manager's
            # allocation_table record (sid, service, demand, holding_s)
            try:
                alloc_reply = protocol.request(
                    target["host"], target["port"],
                    {"type": "ALLOCATE", "sid": sid,
                     "service": inst["service"], "demand": demand,
                     "holding_s": inst["holding_s"]},
                    counters=self.counters, auth_key=self.auth_key,
                )
            except (ConnectionError, OSError, TimeoutError):
                alloc_reply = {"type": "ALLOCATE_FAIL"}
            if alloc_reply.get("type") != "ALLOCATE_OK":
                with self.lock:
                    for r in target["capacity"]:
                        target["allocated"][r] -= demand.get(r, 0)
                    self.topology.release_path(new_path, demand["bw"])
                    inst["state"] = "RUNNING"  # remain at (dead) source
                    self.migrations_failed += 1
                return False
            # commit: drop old reservation (the source is dead anyway,
            # but its allocation_vector still holds it -- we must free
            # it from the manager's perspective)
            with self.lock:
                for r in source["capacity"]:
                    source["allocated"][r] = max(0, source["allocated"][r] - demand.get(r, 0))
                self.topology.release_path(inst["path"], demand["bw"])
                inst["site"] = target_site_id
                inst["path"] = new_path
                inst["path_latency_ms"] = new_lat
                inst["state"] = "RUNNING"
                self._wal_append({"op": "MIGRATE", "sid": sid,
                                  "from_site": source_id, "to_site": target_site_id,
                                  "new_path": new_path, "path_latency_ms": new_lat,
                                  "recovery": True})
                self.migrations_succeeded += 1
            print(f"[manager] evacuated {sid}: {source_id} (SUSPECT) -> {target_site_id} "
                  f"[recovery, lost scheduler bookkeeping]", flush=True)
            return True

        # Live migration: source is ACTIVE, do the three-step dance
        # step 1: ask the source for a snapshot (lock released for the round trip)
        try:
            snap_reply = protocol.request(
                source["host"], source["port"],
                {"type": "INSTANCE_MIGRATE", "sid": sid},
                counters=self.counters, auth_key=self.auth_key,
            )
        except (ConnectionError, OSError, TimeoutError):
            snap_reply = {"type": "MIGRATE_FAIL", "reason": "source_unreachable"}
        if snap_reply.get("type") != "MIGRATE_SNAPSHOT":
            with self.lock:
                # rollback target reservation
                for r in target["capacity"]:
                    target["allocated"][r] -= demand.get(r, 0)
                self.topology.release_path(new_path, demand["bw"])
                inst["state"] = "RUNNING"
                self.migrations_failed += 1
            return False
        snapshot = snap_reply["snapshot"]

        # step 2: reincarnate at the target
        try:
            alloc_reply = protocol.request(
                target["host"], target["port"],
                {"type": "MIGRATE_ALLOCATE", "snapshot": snapshot},
                counters=self.counters, auth_key=self.auth_key,
            )
        except (ConnectionError, OSError, TimeoutError):
            alloc_reply = {"type": "ALLOCATE_FAIL", "reason": "target_unreachable"}
        if alloc_reply.get("type") != "ALLOCATE_OK":
            with self.lock:
                for r in target["capacity"]:
                    target["allocated"][r] -= demand.get(r, 0)
                self.topology.release_path(new_path, demand["bw"])
                inst["state"] = "RUNNING"
                self.migrations_failed += 1
            return False

        # step 3: success -- commit on the manager, release on the source
        with self.lock:
            # release the OLD reservation (old site + old path)
            for r in source["capacity"]:
                source["allocated"][r] = max(0, source["allocated"][r] - demand.get(r, 0))
            self.topology.release_path(inst["path"], demand["bw"])
            # commit the new reservation in the allocation table
            inst["site"] = target_site_id
            inst["path"] = new_path
            inst["path_latency_ms"] = new_lat
            inst["state"] = "RUNNING"
            self._wal_append({"op": "MIGRATE", "sid": sid,
                              "from_site": source_id, "to_site": target_site_id,
                              "new_path": new_path, "path_latency_ms": new_lat})
            self.migrations_succeeded += 1

        # step 4: tell the source it can drop its local copy now
        try:
            protocol.request(
                source["host"], source["port"],
                {"type": "MIGRATE_COMPLETE", "sid": sid},
                counters=self.counters, auth_key=self.auth_key,
            )
        except (ConnectionError, OSError, TimeoutError):
            # not fatal -- the source's reaper will eventually reap the
            # stale copy when its holding_s expires, and the manager
            # has already updated its allocation table.
            pass
        print(f"[manager] migrated {sid}: {source_id} -> {target_site_id}", flush=True)
        return True

    def _on_cli_migrate(self, msg):
        """CLI-driven migration: {type: MIGRATE, sid, target?}."""
        sid = msg["sid"]
        target = msg.get("target")
        ok = self._migrate_instance(sid, target_site_id=target)
        if ok:
            return {"type": "MIGRATE_OK", "sid": sid}
        return {"type": "MIGRATE_FAIL", "sid": sid}

    # ------------------------------------------------------------ STATUS
    def _status(self):
        with self.lock:
            return {
                "type": "STATUS_OK",
                "policy": self.policy,
                "sites": {sid: {
                    "tier": s["tier"], "status": s["status"], "capacity": s["capacity"],
                    "allocated": s["allocated"], "last_fairness": s["last_fairness"],
                    "last_slot_util": s["last_slot_util"],
                    "host": s["host"], "port": s["port"],
                    "workers": s["workers"],
                } for sid, s in self.sites.items()},
                "running_instances": len(self.allocation_table),
                "instances": [
                    {"sid": i["sid"], "service": i["service"], "site": i["site"],
                     "ingress": i["ingress"], "demand": dict(i["demand"]),
                     "state": i["state"], "path": i["path"],
                     "path_latency_ms": i["path_latency_ms"],
                     "holding_s": i["holding_s"],
                     "admitted_at": i["admitted_at"]}
                    for i in self.allocation_table.values()
                ],
                "offered": dict(self.offered),
                "blocked": dict(self.blocked),
                "admitted": dict(self.admitted),
                "link_utilisation": self.topology.link_utilisation(),
                "counters": self.counters.snapshot(),
                "migrations": {
                    "attempted": self.migrations_attempted,
                    "succeeded": self.migrations_succeeded,
                    "failed": self.migrations_failed,
                },
                "admission_in_flight": self._admission_in_flight,
            }

    # -------------------------------------------------------- background
    def _sampler_loop(self):
        while not self._stop.is_set():
            time.sleep(self.heartbeat_interval)
            with self.lock:
                sample = {
                    "ts": time.time(),
                    "sites": {sid: {
                        "cpu_util": s["allocated"]["cpu"] / s["capacity"]["cpu"] if s["capacity"]["cpu"] else 0,
                        "mem_util": s["allocated"]["mem"] / s["capacity"]["mem"] if s["capacity"]["mem"] else 0,
                        "bw_util": s["allocated"]["bw"] / s["capacity"]["bw"] if s["capacity"]["bw"] else 0,
                    } for sid, s in self.sites.items()},
                    "links": self.topology.link_utilisation(),
                }
                self.utilisation_samples.append(sample)

    def _failure_detector_loop(self):
        """M3: when a site is demoted to SUSPECT, evacuate all its live
        instances to healthy sites before they expire. Closes the M1
        limitation 'failure detection only -- instances not recovered
        or migrated'."""
        timeout = self.heartbeat_interval * self.failure_timeout_hb
        while not self._stop.is_set():
            time.sleep(self.heartbeat_interval)
            now = time.time()
            to_evacuate = []
            with self.lock:
                for sid, s in self.sites.items():
                    if s["status"] == "ACTIVE" and now - s["last_heartbeat"] > timeout:
                        s["status"] = "SUSPECT"
                        print(f"[manager] {sid} -> SUSPECT (no heartbeat for {now - s['last_heartbeat']:.1f}s)", flush=True)
                        self._wal_append({"op": "STATUS", "site_id": sid, "status": "SUSPECT"})
                # collect live instances on SUSPECT sites for evacuation
                for inst in self.allocation_table.values():
                    if inst["state"] != "RUNNING":
                        continue
                    site = self.sites.get(inst["site"])
                    if site is not None and site["status"] == "SUSPECT":
                        to_evacuate.append(inst["sid"])
            # do the actual migrations OUTSIDE the lock to allow other
            # admissions to proceed
            for sid in to_evacuate:
                ok = self._migrate_instance(sid)
                if not ok:
                    print(f"[manager] evacuation failed for {sid} (no target)", flush=True)
                else:
                    print(f"[manager] evacuated {sid} off SUSPECT site", flush=True)

    # ------------------------------------------------- M4: persistence ---
    def _wal_append(self, op: dict):
        """Append one state-changing op to the WAL. Called under self.lock."""
        op = dict(op)
        op["ts"] = time.time()
        try:
            with open(self._wal_path, "a") as f:
                f.write(json.dumps(op) + "\n")
        except OSError as e:
            print(f"[manager] WAL write failed: {e}", flush=True)

    def _snapshot_locked(self, label: str = ""):
        """Write a snapshot of the manager's mutable state to disk.
        Called under self.lock."""
        state = {
            "label": label,
            "ts": time.time(),
            "policy": self.policy,
            "next_sid": self._next_sid,
            "sites": {sid: {
                "capacity": s["capacity"], "allocated": s["allocated"],
                "status": s["status"], "tier": s["tier"],
                "host": s["host"], "port": s["port"],
            } for sid, s in self.sites.items()},
            "allocation_table": {
                sid: {k: v for k, v in inst.items() if k != "path"} | {"path": inst["path"]}
                for sid, inst in self.allocation_table.items()
            },
            "offered": dict(self.offered),
            "blocked": dict(self.blocked),
            "admitted": dict(self.admitted),
            "migrations": {
                "attempted": self.migrations_attempted,
                "succeeded": self.migrations_succeeded,
                "failed": self.migrations_failed,
            },
            "link_reserved": {str(k): v for k, v in self.topology.reserved.items()},
        }
        try:
            with open(self._snap_path, "w") as f:
                json.dump(state, f, indent=2)
        except OSError as e:
            print(f"[manager] snapshot write failed: {e}", flush=True)

    def _snapshot_loop(self):
        while not self._stop.is_set():
            time.sleep(self.snapshot_interval)
            with self.lock:
                self._snapshot_locked(f"periodic_{int(time.time())}")

    def _recover(self, recover_dir: str):
        """M4: on startup, replay state from the latest snapshot plus
        the WAL tail. Best-effort -- if no snapshot/WAL is found, the
        manager starts fresh (which is always safe for a new run)."""
        snap_path = os.path.join(recover_dir, "manager_state.json")
        wal_path = os.path.join(recover_dir, "wal.jsonl")
        if os.path.exists(snap_path):
            try:
                with open(snap_path) as f:
                    state = json.load(f)
                self.policy = state.get("policy", self.policy)
                self._next_sid = state.get("next_sid", 1)
                for sid, s in state.get("sites", {}).items():
                    self.sites[sid] = {
                        "site_id": sid, "host": s["host"], "port": s["port"],
                        "tier": s["tier"], "capacity": dict(s["capacity"]),
                        "allocated": dict(s["allocated"]),
                        "workers": 1, "status": s.get("status", "SUSPECT"),
                        "last_heartbeat": time.time(),
                        "last_fairness": None, "last_slot_util": None,
                    }
                    # mark recovered sites as SUSPECT so the failure
                    # detector re-checks them; if they re-REGISTER,
                    # _on_heartbeat will flip them back to ACTIVE.
                for sid, inst in state.get("allocation_table", {}).items():
                    inst["state"] = "RUNNING"
                    self.allocation_table[sid] = inst
                for k, v in state.get("link_reserved", {}).items():
                    # k is a string repr of a frozenset; reconstruct
                    parts = k.replace("frozenset(", "").replace(")", "").replace("'", "").split(", ")
                    key = frozenset(p.strip() for p in parts if p.strip())
                    if key in self.topology.reserved:
                        self.topology.reserved[key] = v
                self.offered = dict(state.get("offered", {"_all": 0}))
                self.blocked = dict(state.get("blocked", {"_all": 0}))
                self.admitted = dict(state.get("admitted", {"_all": 0}))
                m = state.get("migrations", {})
                self.migrations_attempted = m.get("attempted", 0)
                self.migrations_succeeded = m.get("succeeded", 0)
                self.migrations_failed = m.get("failed", 0)
                print(f"[manager] recovered snapshot: {len(self.allocation_table)} live instances, {len(self.sites)} sites", flush=True)
            except (OSError, json.JSONDecodeError) as e:
                print(f"[manager] snapshot read failed: {e}", flush=True)

        if os.path.exists(wal_path):
            try:
                with open(wal_path) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            op = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        self.recover_applied += 1
                        self._replay_op(op)
            except OSError as e:
                print(f"[manager] WAL read failed: {e}", flush=True)

    def _replay_op(self, op: dict):
        """Replay one WAL op into the manager's mutable state. Only the
        subset of ops that change the allocation table or site vector
        need replaying; metric-only ops (admit/exit/blocked) just bump
        counters, which we already restored from the snapshot."""
        kind = op.get("op")
        if kind == "REGISTER":
            sid = op["site_id"]
            if sid not in self.sites:
                self.sites[sid] = {
                    "site_id": sid, "host": op["host"], "port": op["port"],
                    "tier": op["tier"], "capacity": dict(op["capacity"]),
                    "allocated": {"cpu": 0, "mem": 0, "bw": 0},
                    "workers": 1, "status": "SUSPECT",
                    "last_heartbeat": time.time(),
                    "last_fairness": None, "last_slot_util": None,
                }
        elif kind == "ADMIT":
            sid = op["sid"]
            self.allocation_table[sid] = {
                "sid": sid, "service": op["service"], "site": op["site"],
                "ingress": op["ingress"], "demand": dict(op["demand"]),
                "path": op["path"], "path_latency_ms": op.get("path_latency_ms", 0),
                "holding_s": op["holding_s"], "admitted_at": op.get("admitted_at", time.time()),
                "state": "RUNNING",
            }
            site = self.sites.get(op["site"])
            if site is not None:
                for r in site["capacity"]:
                    site["allocated"][r] += op["demand"].get(r, 0)
            self._next_sid = max(self._next_sid, int(sid.lstrip("S")) + 1)
        elif kind == "EXIT":
            sid = op["sid"]
            inst = self.allocation_table.pop(sid, None)
            if inst is not None:
                site = self.sites.get(inst["site"])
                if site is not None:
                    for r in site["capacity"]:
                        site["allocated"][r] = max(0, site["allocated"][r] - inst["demand"].get(r, 0))
        elif kind == "MIGRATE":
            sid = op["sid"]
            inst = self.allocation_table.get(sid)
            if inst is not None:
                old_site = self.sites.get(inst["site"])
                if old_site is not None:
                    for r in old_site["capacity"]:
                        old_site["allocated"][r] = max(0, old_site["allocated"][r] - inst["demand"].get(r, 0))
                new_site = self.sites.get(op["to_site"])
                if new_site is not None:
                    for r in new_site["capacity"]:
                        new_site["allocated"][r] += inst["demand"].get(r, 0)
                inst["site"] = op["to_site"]
                inst["path"] = op.get("new_path", inst["path"])
        elif kind == "RESIZE":
            sid = op["sid"]
            inst = self.allocation_table.get(sid)
            if inst is not None:
                old_cpu = inst["demand"]["cpu"]
                new_cpu = op["new_cpu"]
                delta = new_cpu - old_cpu
                inst["demand"]["cpu"] = new_cpu
                site = self.sites.get(inst["site"])
                if site is not None:
                    site["allocated"]["cpu"] += delta
        elif kind == "STATUS":
            sid = op["site_id"]
            site = self.sites.get(sid)
            if site is not None:
                site["status"] = op["status"]

    # ------------------------------------------------------------ report
    def _write_report(self):
        os.makedirs(self.results_dir, exist_ok=True)
        with self.lock:
            self._snapshot_locked("final_report")
            report = {
                "policy": self.policy,
                "offered": dict(self.offered),
                "blocked": dict(self.blocked),
                "admitted": dict(self.admitted),
                "blocking_probability": {
                    k: (self.blocked.get(k, 0) / v if v else 0.0) for k, v in self.offered.items()
                },
                "blocked_reasons": self.blocked_reasons,
                "decision_time_ms": {
                    "mean": sum(self.decision_times_ms) / len(self.decision_times_ms) if self.decision_times_ms else None,
                    "max": max(self.decision_times_ms) if self.decision_times_ms else None,
                    "count": len(self.decision_times_ms),
                },
                "link_utilisation_final": self.topology.link_utilisation(),
                "counters": self.counters.snapshot(),
                "sites_final": {sid: {"capacity": s["capacity"], "allocated": s["allocated"]} for sid, s in self.sites.items()},
                "migrations": {
                    "attempted": self.migrations_attempted,
                    "succeeded": self.migrations_succeeded,
                    "failed": self.migrations_failed,
                },
                "admission_concurrent": True,  # M3
                "auth_enabled": self.auth_key is not None,  # M4
                "wal_path": self._wal_path,
                "snapshot_path": self._snap_path,
                "recover_applied": self.recover_applied,
                "written_at": datetime.now().isoformat(),
            }
        with open(os.path.join(self.results_dir, "manager_report.json"), "w") as f:
            json.dump(report, f, indent=2)
        with open(os.path.join(self.results_dir, "instances_log.json"), "w") as f:
            json.dump(self.completed_instances, f, indent=2)
        with open(os.path.join(self.results_dir, "heartbeat_log.json"), "w") as f:
            json.dump(self.heartbeat_log, f, indent=2)
        with open(os.path.join(self.results_dir, "utilisation_timeseries.json"), "w") as f:
            json.dump(self.utilisation_samples, f, indent=2)
        print(f"[manager] report written to {self.results_dir}", flush=True)


def main():
    ap = argparse.ArgumentParser(description="TeleRM Global Resource Manager")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--results-dir", default="results/latest")
    ap.add_argument("--policy", default=None, choices=["edge_first", "least_loaded", "best_fit"])
    ap.add_argument("--auth-key", default=None,
                    help="shared secret for HMAC message authentication (M4)")
    ap.add_argument("--recover", default=None,
                    help="recover state from this results directory (M4)")
    args = ap.parse_args()

    with open(args.config) as f:
        config = json.load(f)

    auth_key = protocol.derive_key(args.auth_key) if args.auth_key else None
    mgr = Manager(config, args.results_dir, policy=args.policy,
                  auth_key=auth_key, recover_dir=args.recover)
    mgr.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        mgr._write_report()
        mgr.stop()
        sys.exit(0)


if __name__ == "__main__":
    main()
