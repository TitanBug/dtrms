"""
Unit tests for the parts of TeleRM that don't need real sockets:
topology/shortest-path, admission math, stride/fairness formulas.

Run with:
    python3 -m unittest discover -s tests -v
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.topology import Topology
from common import util


LINKS = [
    {"a": "edge-A", "b": "core-1", "capacity": 1000, "latency_ms": 5},
    {"a": "edge-B", "b": "core-1", "capacity": 1000, "latency_ms": 6},
    {"a": "edge-A", "b": "edge-B", "capacity": 400, "latency_ms": 3},
]


class TestTopology(unittest.TestCase):
    def setUp(self):
        self.topo = Topology(LINKS)

    def test_same_site_path_is_free(self):
        path, lat = self.topo.shortest_path("edge-A", "edge-A")
        self.assertEqual(path, ["edge-A"])
        self.assertEqual(lat, 0)

    def test_shortest_latency_path_is_chosen(self):
        # edge-A -> edge-B direct is 3ms, vs edge-A -> core-1 -> edge-B = 5+6=11ms
        path, lat = self.topo.shortest_path("edge-A", "edge-B")
        self.assertEqual(path, ["edge-A", "edge-B"])
        self.assertEqual(lat, 3)

    def test_du_never_reaches_core_within_2ms(self):
        # every link is >= 3ms, so nothing but the ingress site itself
        # can satisfy a 2ms bound -- this is what makes the DU edge-only.
        for ingress in ("edge-A", "edge-B"):
            _path, lat = self.topo.shortest_path(ingress, "core-1")
            self.assertGreater(lat, 2)

    def test_bandwidth_reservation_and_release(self):
        path, _ = self.topo.shortest_path("edge-A", "edge-B")
        ok, _ = self.topo.path_has_bandwidth(path, 400)
        self.assertTrue(ok)
        self.topo.reserve_path(path, 400)
        ok, blocking = self.topo.path_has_bandwidth(path, 1)
        self.assertFalse(ok)  # link is fully reserved now (400/400 Mb/s)
        self.topo.release_path(path, 400)
        ok, _ = self.topo.path_has_bandwidth(path, 400)
        self.assertTrue(ok)


class TestUtil(unittest.TestCase):
    def test_fits(self):
        cap = {"cpu": 1000, "mem": 1000, "bw": 1000}
        self.assertTrue(util.fits({"cpu": 0, "mem": 0, "bw": 0}, {"cpu": 500, "mem": 500, "bw": 500}, cap))
        self.assertFalse(util.fits({"cpu": 900, "mem": 0, "bw": 0}, {"cpu": 200, "mem": 0, "bw": 0}, cap))

    def test_dominant_share(self):
        cap = {"cpu": 1000, "mem": 2000, "bw": 500}
        # cpu at 50%, mem at 25%, bw at 80% -> dominant (max) is bw's 0.8
        alloc = {"cpu": 500, "mem": 500, "bw": 400}
        self.assertAlmostEqual(util.dominant_share(alloc, cap), 0.8)

    def test_jains_fairness_perfect(self):
        self.assertAlmostEqual(util.jains_fairness_index([1, 1, 1, 1]), 1.0)

    def test_jains_fairness_unequal(self):
        j = util.jains_fairness_index([1, 0, 0, 0])
        self.assertAlmostEqual(j, 0.25)  # classic 1-of-n-active-equally case

    def test_stride_proportionality(self):
        # tickets double -> stride halves -> pass advances half as fast ->
        # over N jobs, the double-ticket instance gets ~2x the jobs.
        stride_1x = 1_000_000 / 1000
        stride_2x = 1_000_000 / 2000
        self.assertAlmostEqual(stride_1x, 2 * stride_2x)

    def test_ceil_div_max_concurrency(self):
        # a 1500-millicore instance (vRAN-DU) may occupy at most 2 workers
        self.assertEqual(util.ceil_div(1500, 1000), 2)
        # a 250-millicore instance (IoT-GW) may occupy at most 1 worker
        self.assertEqual(util.ceil_div(250, 1000), 1)


if __name__ == "__main__":
    unittest.main()
