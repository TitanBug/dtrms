#!/usr/bin/env python3
"""
TeleRM traffic generator (design doc 5.3): Poisson arrivals against the
service catalogue, exponential holding times, and resize follow-ups
against a random accepted instance. Everything is seeded for
reproducibility.

Run standalone:
    python3 generator.py --config config.json --results-dir results/run1
"""
import argparse
import collections
import json
import os
import random
import threading
import time

from common import protocol
from common.counters import Counters


def weighted_choice(rng, items_shares):
    names, shares = zip(*items_shares)
    return rng.choices(names, weights=shares, k=1)[0]


class Generator:
    def __init__(self, config, manager_host, manager_port, results_dir, seed=None,
                 duration=None, rate=None, auth_key: bytes = None):
        self.config = config
        self.manager_host = manager_host
        self.manager_port = manager_port
        self.auth_key = auth_key
        self.results_dir = results_dir
        wl = config["workload"]
        self.duration_s = duration if duration is not None else wl["duration_s"]
        self.rate = rate if rate is not None else wl["arrival_rate_per_s"]
        self.mean_holding_s = wl["mean_holding_s"]
        self.resize_prob = wl["resize_prob"]
        self.resize_factors = wl["resize_factors"]
        self.ingress_sites = wl["ingress_sites"]
        self.services = config["services"]
        self.rng = random.Random(seed if seed is not None else config.get("seed", 42))
        self.counters = Counters()

        self.log = []                 # every REQUEST/reply pair
        self.accepted_sids = collections.deque(maxlen=50)  # rolling window for RESIZE targets
        self._lock = threading.Lock()
        self._resize_threads = []
        self._req_counter = 0

    def _next_req_id(self):
        self._req_counter += 1
        return self._req_counter

    def _send_request(self, service, ingress):
        spec = self.services[service]
        holding_s = self.rng.expovariate(1.0 / self.mean_holding_s)
        req = {
            "type": "REQUEST", "req_id": self._next_req_id(), "service": service, "ingress": ingress,
            "demand": {"cpu": spec["cpu"], "mem": spec["mem"], "bw": spec["bw"]},
            "max_latency_ms": spec["max_latency_ms"], "holding_s": round(holding_s, 3),
        }
        t0 = time.perf_counter()
        try:
            reply = protocol.request(self.manager_host, self.manager_port, req, counters=self.counters, timeout=5.0, auth_key=self.auth_key)
        except (ConnectionError, OSError, TimeoutError) as e:
            reply = {"type": "ERROR", "error": str(e)}
        rtt_ms = (time.perf_counter() - t0) * 1000

        record = {"req": req, "reply": reply, "rtt_ms": round(rtt_ms, 3), "ts": time.time()}
        with self._lock:
            self.log.append(record)
            if reply.get("type") == "ACCEPTED":
                self.accepted_sids.append(reply["sid"])
        return reply, holding_s

    def _maybe_schedule_resize(self, holding_s):
        if self.rng.random() >= self.resize_prob:
            return
        with self._lock:
            if not self.accepted_sids:
                return
            target_sid = self.rng.choice(self.accepted_sids)
        factor = self.rng.choice(self.resize_factors)
        # fire the resize at a random point within the (likely still-running) instance's life
        delay = self.rng.uniform(0.2, max(0.3, holding_s * 0.6))

        def _fire():
            time.sleep(delay)
            req = {"type": "RESIZE", "req_id": self._next_req_id(), "sid": target_sid, "factor": factor}
            try:
                reply = protocol.request(self.manager_host, self.manager_port, req, counters=self.counters, timeout=5.0, auth_key=self.auth_key)
            except (ConnectionError, OSError, TimeoutError) as e:
                reply = {"type": "ERROR", "error": str(e)}
            with self._lock:
                self.log.append({"req": req, "reply": reply, "ts": time.time()})

        th = threading.Thread(target=_fire, daemon=True)
        th.start()
        self._resize_threads.append(th)

    def run(self):
        items_shares = list(self.services[s]["share"] for s in self.services)
        service_names = list(self.services.keys())
        name_share_pairs = list(zip(service_names, items_shares))

        t_end = self.duration_s
        t = 0.0
        n_sent = 0
        print(f"[generator] starting: rate={self.rate}/s duration={self.duration_s}s seed={self.config.get('seed')}", flush=True)
        start_wall = time.time()
        while True:
            gap = self.rng.expovariate(self.rate)
            t += gap
            if t > t_end:
                break
            # sleep until this arrival's simulated time, in real wall-clock time
            target_wall = start_wall + t
            sleep_for = target_wall - time.time()
            if sleep_for > 0:
                time.sleep(sleep_for)

            service = weighted_choice(self.rng, name_share_pairs)
            ingress = self.rng.choice(self.ingress_sites)
            reply, holding_s = self._send_request(service, ingress)
            n_sent += 1
            if reply.get("type") == "ACCEPTED":
                self._maybe_schedule_resize(holding_s)

        print(f"[generator] done sending: {n_sent} requests issued over {t:.1f}s (sim time)", flush=True)
        # let in-flight resize threads finish
        for th in self._resize_threads:
            th.join(timeout=max(1.0, self.mean_holding_s))

        self._write_log()
        return n_sent

    def _write_log(self):
        os.makedirs(self.results_dir, exist_ok=True)
        with open(os.path.join(self.results_dir, "generator_log.json"), "w") as f:
            json.dump(self.log, f, indent=2)

        accepted = sum(1 for r in self.log if r["reply"].get("type") == "ACCEPTED")
        blocked = sum(1 for r in self.log if r["reply"].get("type") == "BLOCKED")
        requests_only = [r for r in self.log if r["req"]["type"] == "REQUEST"]
        summary = {
            "requests_sent": len(requests_only),
            "accepted": accepted,
            "blocked": blocked,
            "blocking_probability": blocked / len(requests_only) if requests_only else None,
            "counters": self.counters.snapshot(),
        }
        with open(os.path.join(self.results_dir, "generator_summary.json"), "w") as f:
            json.dump(summary, f, indent=2)
        print(f"[generator] {summary}", flush=True)


def main():
    ap = argparse.ArgumentParser(description="TeleRM traffic generator")
    ap.add_argument("--config", default="config.json")
    ap.add_argument("--results-dir", default="results/latest")
    ap.add_argument("--manager-host", default=None)
    ap.add_argument("--manager-port", type=int, default=None)
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--duration", type=float, default=None)
    ap.add_argument("--rate", type=float, default=None)
    ap.add_argument("--auth-key", default=None,
                    help="shared secret for HMAC message authentication (M4)")
    args = ap.parse_args()

    with open(args.config) as f:
        config = json.load(f)
    mhost = args.manager_host or config["manager"]["host"]
    mport = args.manager_port or config["manager"]["port"]
    auth_key = protocol.derive_key(args.auth_key) if args.auth_key else None

    gen = Generator(config, mhost, mport, args.results_dir, seed=args.seed, duration=args.duration, rate=args.rate, auth_key=auth_key)
    gen.run()


if __name__ == "__main__":
    main()
