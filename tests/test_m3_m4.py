"""
Unit tests for the Milestone-3 and Milestone-4 additions:
  - M3: migration snapshot round-trip (Instance.snapshot / from_snapshot)
  - M3: migration target selection (_pick_migration_target)
  - M3: WAL append + replay produces the same allocation table
  - M3: HMAC signing / verification on the wire protocol
  - M4: snapshot serialises all mutable state needed for recovery

These tests do NOT use real sockets -- they exercise the pure-logic
helpers. End-to-end migration across real sockets is verified by an
integration smoke test (see `run_demo.py --policy edge_first --seed 42`).

Run with:
    python3 -m unittest discover -s tests -v
"""
import importlib.util
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import protocol, util
from common.counters import Counters
from common.topology import Topology

# Python's stdlib ships a `site` module (site-packages setup); load our
# project's site.py explicitly by file path to avoid the shadow.
_HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


site_mod = _load_module("telerm_site", os.path.join(_HERE, "site.py"))
manager_mod = _load_module("telerm_manager", os.path.join(_HERE, "manager.py"))


LINKS = [
    {"a": "edge-A", "b": "core-1", "capacity": 1000, "latency_ms": 5},
    {"a": "edge-B", "b": "core-1", "capacity": 1000, "latency_ms": 6},
    {"a": "edge-A", "b": "edge-B", "capacity": 400, "latency_ms": 3},
]

CONFIG = {
    "seed": 42,
    "manager": {"host": "127.0.0.1", "port": 6000},
    "sites": {
        "edge-A": {"tier": "edge", "host": "127.0.0.1", "port": 6101,
                   "cpu": 4000, "mem": 4096, "bw": 2000, "workers": 2},
        "edge-B": {"tier": "edge", "host": "127.0.0.1", "port": 6102,
                   "cpu": 4000, "mem": 4096, "bw": 2000, "workers": 2},
        "core-1": {"tier": "core", "host": "127.0.0.1", "port": 6103,
                   "cpu": 12000, "mem": 16384, "bw": 10000, "workers": 4},
    },
    "links": LINKS,
    "services": {
        "vRAN-DU":    {"cpu": 1500, "mem": 512, "bw": 300, "max_latency_ms": 2,  "share": 0.20},
        "UPF":        {"cpu": 1000, "mem": 512, "bw": 400, "max_latency_ms": 10, "share": 0.20},
        "IMS-CSCF":   {"cpu": 500,  "mem": 256, "bw": 50,  "max_latency_ms": 50, "share": 0.20},
        "Transcoder": {"cpu": 2000, "mem": 1024, "bw": 200, "max_latency_ms": 50, "share": 0.15},
        "IoT-GW":     {"cpu": 250,  "mem": 128, "bw": 20,  "max_latency_ms": 100,"share": 0.25},
    },
    "workload": {"arrival_rate_per_s": 2.0, "duration_s": 30, "mean_holding_s": 8.0,
                 "resize_prob": 0.25, "resize_factors": [0.5, 1.5, 2.0],
                 "ingress_sites": ["edge-A", "edge-B"]},
    "placement_policy": "edge_first",
    "heartbeat_interval_s": 1.0,
    "failure_timeout_heartbeats": 3,
    "fairness_saturation_threshold": 0.9,
    "job_service_ms": 6,
    "snapshot_interval_s": 5,
}


# ----------------------------------------------------------------- M3
class TestInstanceSnapshot(unittest.TestCase):
    def test_snapshot_round_trip(self):
        """An Instance must survive a snapshot/from_snapshot round-trip
        with all scheduler-relevant fields preserved."""
        inst = site_mod.Instance("S00001", "UPF", {"cpu": 1000, "mem": 512, "bw": 400}, 8.0)
        inst.jobs_done = 17
        inst.busy_s = 1.234
        inst.latency_sum_ms = 99.5
        inst.pass_value = 12345.6
        original_expires = inst.expires_at

        snap = inst.snapshot()
        self.assertEqual(snap["sid"], "S00001")
        self.assertEqual(snap["service"], "UPF")
        self.assertEqual(snap["jobs_done"], 17)
        self.assertAlmostEqual(snap["pass_value"], 12345.6)
        self.assertAlmostEqual(snap["expires_at"], original_expires)

        reincarnated = site_mod.Instance.from_snapshot(snap)
        self.assertEqual(reincarnated.sid, "S00001")
        self.assertEqual(reincarnated.service, "UPF")
        self.assertEqual(reincarnated.jobs_done, 17)
        self.assertAlmostEqual(reincarnated.pass_value, 12345.6)
        self.assertAlmostEqual(reincarnated.expires_at, original_expires)

    def test_migrated_instance_keeps_pass_value(self):
        """A migrated instance must NOT reset pass_value to 0; otherwise
        it would starve other instances for a window after migration
        (it would always be picked next until it catches up)."""
        inst = site_mod.Instance("S00002", "IMS-CSCF", {"cpu": 500, "mem": 256, "bw": 50}, 4.0)
        inst.pass_value = 99.0  # mid-stride
        snap = inst.snapshot()
        reincarnated = site_mod.Instance.from_snapshot(snap)
        self.assertAlmostEqual(reincarnated.pass_value, 99.0)
        self.assertNotEqual(reincarnated.pass_value, 0.0)


# ----------------------------------------------------------------- M3
class TestMigrationTargetPicker(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.mgr = manager_mod.Manager(CONFIG, self.tmp, policy="edge_first")
        # register two sites by hand
        for sid in ("edge-A", "edge-B", "core-1"):
            scfg = CONFIG["sites"][sid]
            self.mgr.sites[sid] = {
                "site_id": sid, "host": scfg["host"], "port": scfg["port"],
                "tier": scfg["tier"], "capacity": {"cpu": scfg["cpu"], "mem": scfg["mem"], "bw": scfg["bw"]},
                "allocated": {"cpu": 0, "mem": 0, "bw": 0},
                "workers": scfg["workers"], "status": "ACTIVE",
                "last_heartbeat": 0, "last_fairness": None, "last_slot_util": None,
            }

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_inst(self, site, ingress="edge-A", service="UPF"):
        return {
            "sid": "S00001", "service": service, "site": site, "ingress": ingress,
            "demand": {"cpu": 1000, "mem": 512, "bw": 400},
            "path": [ingress], "path_latency_ms": 0,
            "holding_s": 8.0, "admitted_at": 0, "state": "RUNNING",
        }

    def test_picks_least_loaded_target(self):
        """When migrating, the manager should prefer the least-loaded
        candidate that satisfies the demand (here core-1, which is
        empty, over edge-B, which is artificially loaded)."""
        inst = self._make_inst("edge-A")
        # load edge-B so it's a worse choice than core-1
        self.mgr.sites["edge-B"]["allocated"] = {"cpu": 3500, "mem": 0, "bw": 0}
        target = self.mgr._pick_migration_target(inst, dict(inst["demand"]))
        self.assertEqual(target, "core-1")

    def test_excludes_source_site(self):
        """The source site can never be its own migration target."""
        inst = self._make_inst("edge-A")
        # exhaust every other site
        self.mgr.sites["edge-B"]["allocated"] = {"cpu": 4000, "mem": 4096, "bw": 2000}
        self.mgr.sites["core-1"]["allocated"] = {"cpu": 12000, "mem": 16384, "bw": 10000}
        target = self.mgr._pick_migration_target(inst, dict(inst["demand"]))
        self.assertIsNone(target)

    def test_skips_suspect_sites(self):
        inst = self._make_inst("edge-A")
        self.mgr.sites["core-1"]["status"] = "SUSPECT"
        target = self.mgr._pick_migration_target(inst, dict(inst["demand"]))
        # core-1 excluded; edge-B should be the only viable target
        self.assertEqual(target, "edge-B")


# ----------------------------------------------------------------- M4
class TestHMAC(unittest.TestCase):
    def test_signing_round_trip(self):
        key = protocol.derive_key("s3cret")
        obj = {"type": "STATUS", "site_id": "edge-A"}
        signed = protocol._sign(obj, key)
        self.assertIn("hmac", signed)
        # verification must not raise
        protocol._verify(signed, key)

    def test_bad_signature_raises(self):
        key = protocol.derive_key("s3cret")
        obj = {"type": "STATUS", "site_id": "edge-A"}
        signed = protocol._sign(obj, key)
        # tamper with a field but keep the (now stale) hmac
        signed["site_id"] = "edge-B"
        with self.assertRaises(ValueError):
            protocol._verify(signed, key)

    def test_unsigned_message_rejected_when_key_set(self):
        key = protocol.derive_key("s3cret")
        obj = {"type": "STATUS", "site_id": "edge-A"}
        with self.assertRaises(ValueError):
            protocol._verify(obj, key)

    def test_no_key_means_no_check(self):
        """When no auth_key is configured (the default), the protocol
        behaves exactly like M1 -- any message passes through."""
        obj = {"type": "STATUS", "site_id": "edge-A"}
        protocol._verify(obj, None)  # must not raise
        out = protocol._sign(obj, None)
        self.assertNotIn("hmac", out)


# ----------------------------------------------------------------- M4
class TestWALReplay(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_register_admit_exit_replays_to_same_state(self):
        """A WAL containing REGISTER + ADMIT + EXIT must replay to the
        same allocation table as the live manager ended with."""
        m = manager_mod.Manager(CONFIG, self.tmp, policy="edge_first")
        m._on_register({"site_id": "edge-A", "host": "127.0.0.1", "port": 6101,
                        "tier": "edge",
                        "capacity": {"cpu": 4000, "mem": 4096, "bw": 2000}})
        m._on_register({"site_id": "core-1", "host": "127.0.0.1", "port": 6103,
                        "tier": "core",
                        "capacity": {"cpu": 12000, "mem": 16384, "bw": 10000}})
        m._next_sid = 1
        # Mock the ALLOCATE round-trip to the site so we don't need a
        # real site process. The site is "alive" enough to reply OK.
        with mock.patch.object(protocol, "request",
                               return_value={"type": "ALLOCATE_OK", "sid": "S00001"}):
            m._on_request({
                "type": "REQUEST", "req_id": 1, "service": "UPF",
                "ingress": "edge-A",
                "demand": {"cpu": 1000, "mem": 512, "bw": 400},
                "max_latency_ms": 10, "holding_s": 4.0,
            })
        # that should have produced an ADMIT in the WAL
        with open(m._wal_path) as f:
            wal = [json.loads(l) for l in f if l.strip()]
        ops = [o["op"] for o in wal]
        self.assertIn("REGISTER", ops)
        self.assertIn("ADMIT", ops)
        # the live allocation table has the instance
        self.assertEqual(len(m.allocation_table), 1)
        live_inst = next(iter(m.allocation_table.values()))
        self.assertEqual(live_inst["service"], "UPF")
        # site allocated vector should reflect the demand
        self.assertEqual(m.sites["edge-A"]["allocated"]["cpu"], 1000)

        # now: simulate a crash by spinning up a NEW manager pointing at
        # the SAME results dir with --recover
        m2 = manager_mod.Manager(CONFIG, self.tmp, policy="edge_first",
                                 recover_dir=self.tmp)
        # the recovery should have rebuilt the same allocation table
        self.assertEqual(len(m2.allocation_table), 1,
                         f"recovery should restore 1 instance, got {len(m2.allocation_table)}")
        live_inst2 = next(iter(m2.allocation_table.values()))
        self.assertEqual(live_inst2["service"], "UPF")
        self.assertEqual(live_inst2["site"], "edge-A")
        self.assertEqual(m2.sites["edge-A"]["allocated"]["cpu"], 1000,
                         "recovery should restore site allocated vector")
        self.assertGreaterEqual(m2.recover_applied, 2,
                                "should have replayed at least REGISTER + ADMIT ops")

    def test_snapshot_round_trip(self):
        """A snapshot must capture enough state for the manager to pick
        up where it left off (allocation table, site vectors, link
        reservations, next_sid)."""
        m = manager_mod.Manager(CONFIG, self.tmp, policy="least_loaded")
        m._on_register({"site_id": "edge-A", "host": "127.0.0.1", "port": 6101,
                        "tier": "edge",
                        "capacity": {"cpu": 4000, "mem": 4096, "bw": 2000}})
        m._on_register({"site_id": "core-1", "host": "127.0.0.1", "port": 6103,
                        "tier": "core",
                        "capacity": {"cpu": 12000, "mem": 16384, "bw": 10000}})
        m._next_sid = 1
        # we can't easily drive _on_request through the network here
        # (no real site to ALLOCATE), but we can synthesise the post-
        # admit state by hand and snapshot it
        m.allocation_table["S00001"] = {
            "sid": "S00001", "service": "UPF", "site": "edge-A", "ingress": "edge-A",
            "demand": {"cpu": 1000, "mem": 512, "bw": 400},
            "path": ["edge-A"], "path_latency_ms": 0,
            "holding_s": 4.0, "admitted_at": 0, "state": "RUNNING",
        }
        m.sites["edge-A"]["allocated"] = {"cpu": 1000, "mem": 512, "bw": 400}
        m.topology.reserve_path(["edge-A"], 400)  # noop since path is one hop
        m._next_sid = 2
        m._snapshot_locked("test_snapshot")

        # reload from the snapshot
        m2 = manager_mod.Manager(CONFIG, self.tmp, policy="least_loaded",
                                 recover_dir=self.tmp)
        self.assertIn("S00001", m2.allocation_table)
        self.assertEqual(m2.allocation_table["S00001"]["service"], "UPF")
        self.assertEqual(m2.sites["edge-A"]["allocated"]["cpu"], 1000)
        self.assertEqual(m2._next_sid, 2)


if __name__ == "__main__":
    unittest.main()
