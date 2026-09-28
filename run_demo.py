#!/usr/bin/env python3
"""
TeleRM demo runner (design doc section 11, "Reproducibility record").

Spawns the manager and every site as separate OS processes (their own
TCP ports, as required by section 3.1), runs the generator, waits for
in-flight instances to drain, then asks the manager to shut down and
prints/writes the final report.

Examples:
    python3 run_demo.py --policy edge_first
    python3 run_demo.py --policy least_loaded --seed 7 --duration 20
"""
import argparse
import copy
import json
import os
import platform
import socket
import subprocess
import sys
import time
from datetime import datetime

from common import protocol

HERE = os.path.dirname(os.path.abspath(__file__))


def wait_for_port(host, port, timeout=10.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.1)
    return False


def main():
    ap = argparse.ArgumentParser(description="Run a full TeleRM S1 demo end to end")
    ap.add_argument("--config", default=os.path.join(HERE, "config.json"))
    ap.add_argument("--policy", default=None, choices=["edge_first", "least_loaded", "best_fit"])
    ap.add_argument("--seed", type=int, default=None)
    ap.add_argument("--duration", type=float, default=None, help="workload duration in seconds")
    ap.add_argument("--rate", type=float, default=None, help="arrivals per second")
    ap.add_argument("--results-dir", default=None)
    ap.add_argument("--drain-timeout", type=float, default=60.0,
                     help="extra seconds to wait for running instances to finish after the generator stops")
    ap.add_argument("--auth-key", default=None,
                    help="shared secret for HMAC message authentication (M4)")
    ap.add_argument("--recover", default=None,
                    help="recover manager state from this results directory on startup (M4)")
    args = ap.parse_args()

    with open(args.config) as f:
        base_config = json.load(f)

    policy = args.policy or base_config.get("placement_policy", "edge_first")
    seed = args.seed if args.seed is not None else base_config.get("seed", 42)

    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    results_dir = args.results_dir or os.path.join(HERE, "results", f"{policy}-seed{seed}-{stamp}")
    os.makedirs(results_dir, exist_ok=True)

    # a per-run config copy, with the seed baked in, for full reproducibility
    run_config = copy.deepcopy(base_config)
    run_config["seed"] = seed
    run_config["placement_policy"] = policy
    if args.duration is not None:
        run_config["workload"]["duration_s"] = args.duration
    if args.rate is not None:
        run_config["workload"]["arrival_rate_per_s"] = args.rate

    with open(os.path.join(results_dir, "config_used.json"), "w") as f:
        json.dump({
            "config": run_config,
            "argv": sys.argv,
            "python_version": sys.version,
            "platform": platform.platform(),
            "command": "python3 run_demo.py " + " ".join(sys.argv[1:]),
            "started_at": datetime.now().isoformat(),
        }, f, indent=2)

    run_config_path = os.path.join(results_dir, "config_used_raw.json")
    with open(run_config_path, "w") as f:
        json.dump(run_config, f, indent=2)

    mhost = run_config["manager"]["host"]
    mport = run_config["manager"]["port"]

    procs = []
    logs = {}

    def spawn(name, cmd):
        logf = open(os.path.join(results_dir, f"{name}.log"), "w")
        p = subprocess.Popen(cmd, cwd=HERE, stdout=logf, stderr=subprocess.STDOUT)
        procs.append((name, p))
        logs[name] = logf
        return p

    # Optional flags appended to every spawned process (M4: auth)
    auth_flag = ["--auth-key", args.auth_key] if args.auth_key else []
    # M4: recovery flag only applies to the manager
    recover_flag = ["--recover", args.recover] if args.recover else []

    print(f"[run_demo] results dir: {results_dir}")
    print(f"[run_demo] policy={policy} seed={seed}")

    try:
        # 1. manager
        spawn("manager", [sys.executable, "manager.py", "--config", run_config_path,
                           "--results-dir", results_dir, "--policy", policy] + auth_flag + recover_flag)
        if not wait_for_port(mhost, mport, timeout=10):
            raise RuntimeError("manager did not come up in time")
        print("[run_demo] manager is up")

        # 2. sites
        for site_id, scfg in run_config["sites"].items():
            spawn(f"site-{site_id}", [sys.executable, "site.py", "--site-id", site_id,
                                       "--config", run_config_path,
                                       "--manager-host", mhost, "--manager-port", str(mport)] + auth_flag)
        for site_id, scfg in run_config["sites"].items():
            if not wait_for_port(scfg["host"], scfg["port"], timeout=10):
                raise RuntimeError(f"site {site_id} did not come up in time")
        time.sleep(0.3)  # let REGISTER land at the manager
        print("[run_demo] all sites are up and registered")

        # 3. generator (run inline so we can block on it directly)
        gen_cmd = [sys.executable, "generator.py", "--config", run_config_path,
                   "--results-dir", results_dir, "--manager-host", mhost, "--manager-port", str(mport)] + auth_flag
        print("[run_demo] running generator ...")
        gen_proc = subprocess.run(gen_cmd, cwd=HERE)
        if gen_proc.returncode != 0:
            print("[run_demo] WARNING: generator exited non-zero", file=sys.stderr)

        # 4. drain: wait for running instances to finish (bounded)
        print("[run_demo] draining in-flight instances ...")
        auth_key_bytes = protocol.derive_key(args.auth_key) if args.auth_key else None
        deadline = time.time() + args.drain_timeout
        while time.time() < deadline:
            try:
                status = protocol.request(mhost, mport, {"type": "STATUS"}, timeout=3.0, auth_key=auth_key_bytes)
                if status.get("running_instances", 0) == 0:
                    break
            except (ConnectionError, OSError, TimeoutError):
                break
            time.sleep(1.0)

        # 5. final status + shutdown
        try:
            final_status = protocol.request(mhost, mport, {"type": "STATUS"}, timeout=3.0, auth_key=auth_key_bytes)
        except (ConnectionError, OSError, TimeoutError):
            final_status = {}
        with open(os.path.join(results_dir, "final_status.json"), "w") as f:
            json.dump(final_status, f, indent=2)

        print("[run_demo] shutting down manager (and sites) ...")
        try:
            protocol.request(mhost, mport, {"type": "SHUTDOWN"}, timeout=3.0, auth_key=auth_key_bytes)
        except (ConnectionError, OSError, TimeoutError):
            pass
        for site_id, scfg in run_config["sites"].items():
            try:
                protocol.request(scfg["host"], scfg["port"], {"type": "SHUTDOWN"}, timeout=3.0, auth_key=auth_key_bytes)
            except (ConnectionError, OSError, TimeoutError):
                pass

        time.sleep(1.0)  # let processes flush and exit on their own

    finally:
        for name, p in procs:
            if p.poll() is None:
                p.terminate()
        for name, p in procs:
            try:
                p.wait(timeout=5)
            except subprocess.TimeoutExpired:
                p.kill()
        for logf in logs.values():
            logf.close()

    _print_summary(results_dir)


def _print_summary(results_dir):
    report_path = os.path.join(results_dir, "manager_report.json")
    if not os.path.exists(report_path):
        print("[run_demo] no manager report found -- check the *.log files in", results_dir)
        return
    with open(report_path) as f:
        report = json.load(f)

    print("\n=== TeleRM run summary ===")
    print(f"policy: {report['policy']}")
    bp = report["blocking_probability"]
    print(f"overall blocking probability: {bp.get('_all', 0):.1%}  "
          f"(offered={report['offered'].get('_all', 0)}, blocked={report['blocked'].get('_all', 0)})")
    for svc, p in sorted(bp.items()):
        if svc == "_all":
            continue
        print(f"  {svc:12s} blocking={p:.1%}  offered={report['offered'].get(svc, 0)}")
    print("link utilisation (final):")
    for link, u in report["link_utilisation_final"].items():
        print(f"  {link:20s} {u['utilisation']:.1%}  ({u['reserved']}/{u['capacity']} Mb/s)")
    dt = report["decision_time_ms"]
    if dt["mean"] is not None:
        print(f"admission decision time: mean={dt['mean']:.2f}ms max={dt['max']:.2f}ms over {dt['count']} requests")
    migs = report.get('migrations', {})
    if migs.get('attempted', 0) > 0 or migs.get('succeeded', 0) > 0:
        print(f"migrations: attempted={migs.get('attempted', 0)} succeeded={migs.get('succeeded', 0)} failed={migs.get('failed', 0)}")
    if report.get('auth_enabled'):
        print(f"auth: HMAC enabled (M4)")
    if report.get('recover_applied', 0) > 0:
        print(f"recovery: replayed {report['recover_applied']} WAL ops from snapshot (M4)")
    print(f"\nfull results in: {results_dir}")


if __name__ == "__main__":
    main()
