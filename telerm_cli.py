#!/usr/bin/env python3
"""
TeleRM CLI (M4 observability tool).

A small operator console for a live TeleRM cluster. Talks to the
manager's TCP port over the same wire protocol as everything else.

Usage examples
--------------
    # live status (one-shot)
    python3 telerm_cli.py --manager 127.0.0.1:6000 status

    # watch live (refresh every 1s)
    python3 telerm_cli.py --manager 127.0.0.1:6000 status --watch

    # topology dump (links + capacities + reservations)
    python3 telerm_cli.py --manager 127.0.0.1:6000 topology

    # list live instances (with their site + latency)
    python3 telerm_cli.py --manager 127.0.0.1:6000 instances

    # force-migrate an instance to a specific target
    python3 telerm_cli.py --manager 127.0.0.1:6000 migrate S00007 core-1

    # let the manager pick the target site (least-loaded)
    python3 telerm_cli.py --manager 127.0.0.1:6000 migrate S00007

    # drain: wait for all running instances to finish
    python3 telerm_cli.py --manager 127.0.0.1:6000 drain

    # shutdown the whole cluster (manager + sites) cleanly
    python3 telerm_cli.py --manager 127.0.0.1:6000 shutdown

    # HMAC-secured cluster:
    python3 telerm_cli.py --manager 127.0.0.1:6000 --auth-key s3cret status

All of these commands are designed to be runnable LIVE during a
presentation — the watch mode is especially handy for showing
migrations happening in real time.
"""
import argparse
import json
import os
import sys
import time

# allow `python3 telerm_cli.py` from inside the project root
HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from common import protocol  # noqa: E402


def _parse_addr(s: str):
    host, _, port = s.partition(":")
    return host, int(port)


def _send(host, port, msg, auth_key=None, timeout=5.0):
    try:
        return protocol.request(host, port, msg, timeout=timeout, auth_key=auth_key)
    except (ConnectionError, OSError, TimeoutError) as e:
        return {"type": "ERROR", "error": str(e)}


def cmd_status(args, auth_key):
    host, port = _parse_addr(args.manager)
    while True:
        s = _send(host, port, {"type": "STATUS"}, auth_key=auth_key)
        if s.get("type") != "STATUS_OK":
            print(f"[cli] error: {s.get('error', s)}", file=sys.stderr)
            if not args.watch:
                return 1
            time.sleep(args.interval)
            continue
        _render_status(s)
        if not args.watch:
            return 0
        time.sleep(args.interval)
        os.system("clear" if os.name == "posix" else "cls")


def _render_status(s):
    print(f"=== TeleRM status @ {time.strftime('%H:%M:%S')} ===")
    print(f"policy: {s.get('policy')}")
    print(f"running instances: {s.get('running_instances')} "
          f"(admission in-flight: {s.get('admission_in_flight', 0)})")
    print()
    print("sites:")
    print(f"  {'site':10s} {'tier':6s} {'status':8s} "
          f"{'cpu':>12s} {'mem':>12s} {'bw':>12s}  slot_util  fairness")
    for sid, site in sorted(s.get("sites", {}).items()):
        cap = site["capacity"]
        alloc = site["allocated"]
        cpu_p = (alloc["cpu"] / cap["cpu"] * 100) if cap["cpu"] else 0
        mem_p = (alloc["mem"] / cap["mem"] * 100) if cap["mem"] else 0
        bw_p = (alloc["bw"] / cap["bw"] * 100) if cap["bw"] else 0
        su = site.get("last_slot_util")
        fair = site.get("last_fairness")
        su_s = f"{su*100:.1f}%" if su is not None else "-"
        fair_s = f"{fair:.3f}" if fair is not None else "-"
        print(f"  {sid:10s} {site['tier']:6s} {site['status']:8s} "
              f"{alloc['cpu']:>5}/{cap['cpu']:<5} ({cpu_p:4.1f}%) "
              f"{alloc['mem']:>5}/{cap['mem']:<5} ({mem_p:4.1f}%) "
              f"{alloc['bw']:>5}/{cap['bw']:<5} ({bw_p:4.1f}%)  "
              f"{su_s:>8s}  {fair_s:>7s}")
    print()
    links = s.get("link_utilisation", {})
    if links:
        print("links:")
        for ln, u in sorted(links.items()):
            cap = u["capacity"]
            res = u["reserved"]
            p = u["utilisation"] * 100
            bar = "#" * int(p / 5)
            print(f"  {ln:20s} {res:>6.0f}/{cap:<6.0f} Mb/s  [{p:5.1f}%] {bar}")
    print()
    mig = s.get("migrations", {})
    if mig:
        print(f"migrations: attempted={mig.get('attempted',0)} "
              f"succeeded={mig.get('succeeded',0)} "
              f"failed={mig.get('failed',0)}")
    counters = s.get("counters", {})
    if counters:
        print(f"manager counters: msgs sent={counters.get('messages_sent',0)} "
              f"recv={counters.get('messages_recv',0)} "
              f"({counters.get('bytes_sent',0)}/{counters.get('bytes_recv',0)} bytes)")
    offered = s.get("offered", {})
    blocked = s.get("blocked", {})
    if offered:
        print(f"admissions: offered={offered.get('_all',0)} "
              f"admitted={s.get('admitted',{}).get('_all',0)} "
              f"blocked={blocked.get('_all',0)}")


def cmd_topo(args, auth_key):
    host, port = _parse_addr(args.manager)
    s = _send(host, port, {"type": "STATUS"}, auth_key=auth_key)
    if s.get("type") != "STATUS_OK":
        print(f"[cli] error: {s.get('error', s)}", file=sys.stderr)
        return 1
    links = s.get("link_utilisation", {})
    print(f"=== TeleRM topology ({len(links)} links) ===")
    print(f"  {'link':20s} {'capacity':>10s} {'reserved':>10s} {'utilisation':>12s}")
    for ln, u in sorted(links.items()):
        print(f"  {ln:20s} {u['capacity']:>10.0f} {u['reserved']:>10.2f} "
              f"{u['utilisation']*100:>11.2f}%")
    return 0


def cmd_instances(args, auth_key):
    """Show live instances. Prefer the manager's direct allocation
    table (M3+ enhancement to STATUS); fall back to querying each site
    for per-instance worker stats if the manager's STATUS reply didn't
    include the per-instance list."""
    host, port = _parse_addr(args.manager)
    s = _send(host, port, {"type": "STATUS"}, auth_key=auth_key)
    if s.get("type") != "STATUS_OK":
        print(f"[cli] error: {s.get('error', s)}", file=sys.stderr)
        return 1
    instances = s.get("instances")
    if instances:
        print(f"=== live instances ({s.get('running_instances',0)} total, from manager) ===")
        print(f"  {'sid':8s} {'service':12s} {'site':10s} {'ingress':8s} "
              f"{'state':10s} {'cpu':>5s} {'mem':>5s} {'bw':>5s}  {'path':20s} {'lat_ms':>6s}")
        for i in sorted(instances, key=lambda x: x.get("sid","")):
            d = i.get("demand", {})
            print(f"  {i.get('sid',''):8s} {i.get('service',''):12s} {i.get('site',''):10s} "
                  f"{i.get('ingress',''):8s} {i.get('state',''):10s} "
                  f"{d.get('cpu',0):5d} {d.get('mem',0):5d} {d.get('bw',0):5d}  "
                  f"{'-'.join(i.get('path',[])):20s} {i.get('path_latency_ms',0):6.1f}")
        mig = s.get("migrations", {})
        print(f"\nmigrations: attempted={mig.get('attempted',0)} "
              f"succeeded={mig.get('succeeded',0)} failed={mig.get('failed',0)}")
        return 0
    # fallback: query each site directly for its instances
    print(f"=== live instances ({s.get('running_instances',0)} total, by querying sites) ===")
    any_inst = False
    for sid, site in sorted(s.get("sites", {}).items()):
        if site["status"] != "ACTIVE":
            continue
        shost, sport = site.get("host"), site.get("port")
        if not shost or not sport:
            continue
        rep = _send(shost, sport, {"type": "STATUS"}, auth_key=auth_key, timeout=2.0)
        if rep.get("type") != "STATUS_OK":
            continue
        for iid, info in rep.get("instances", {}).items():
            any_inst = True
            print(f"  {sid:10s} {site['tier']:6s}  ACTIVE     "
                  f"sid={iid} service={info.get('service'):10s} "
                  f"tickets={info.get('tickets'):4d} jobs_done={info.get('jobs_done'):4d} "
                  f"expires_in={info.get('expires_in_s',0):.1f}s")
    if not any_inst:
        print("  (no live instances right now)")
    mig = s.get("migrations", {})
    print(f"\nmigrations: attempted={mig.get('attempted',0)} "
          f"succeeded={mig.get('succeeded',0)} failed={mig.get('failed',0)}")
    return 0


def cmd_migrate(args, auth_key):
    host, port = _parse_addr(args.manager)
    msg = {"type": "MIGRATE", "sid": args.sid}
    if args.target:
        msg["target"] = args.target
    rep = _send(host, port, msg, auth_key=auth_key, timeout=10.0)
    if rep.get("type") == "MIGRATE_OK":
        print(f"[cli] migrated {args.sid}"
              f"{(' -> ' + args.target) if args.target else ' (auto)'}")
        return 0
    print(f"[cli] migration failed: {rep}", file=sys.stderr)
    return 1


def cmd_drain(args, auth_key):
    host, port = _parse_addr(args.manager)
    deadline = time.time() + args.timeout
    last = 0
    while time.time() < deadline:
        s = _send(host, port, {"type": "STATUS"}, auth_key=auth_key, timeout=3.0)
        n = s.get("running_instances", 0)
        if n != last:
            print(f"[cli] running instances: {n}")
            last = n
        if n == 0:
            print("[cli] drained.")
            return 0
        time.sleep(1.0)
    print(f"[cli] drain timed out after {args.timeout}s", file=sys.stderr)
    return 1


def cmd_shutdown(args, auth_key):
    host, port = _parse_addr(args.manager)
    rep = _send(host, port, {"type": "SHUTDOWN"}, auth_key=auth_key, timeout=3.0)
    print(f"[cli] manager shutdown: {rep}")
    # also try to shut down each site if we can read its address first
    s = _send(host, port, {"type": "STATUS"}, auth_key=auth_key, timeout=1.0)
    for sid, site in s.get("sites", {}).items():
        sh, sp = site.get("host"), site.get("port")
        if not sh or not sp:
            continue
        r = _send(sh, sp, {"type": "SHUTDOWN"}, auth_key=auth_key, timeout=2.0)
        print(f"[cli] site {sid} shutdown: {r.get('type')}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="TeleRM operator CLI (M4)")
    ap.add_argument("--manager", required=True, help="manager address host:port")
    ap.add_argument("--auth-key", default=None, help="HMAC shared secret (M4)")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_status = sub.add_parser("status", help="show cluster status")
    p_status.add_argument("--watch", action="store_true", help="refresh every --interval")
    p_status.add_argument("--interval", type=float, default=1.0)

    p_topo = sub.add_parser("topology", help="show topology + link utilisation")
    p_inst = sub.add_parser("instances", help="list live instances (queries sites too)")
    p_mig = sub.add_parser("migrate", help="migrate an instance (M3)")
    p_mig.add_argument("sid", help="instance sid, e.g. S00007")
    p_mig.add_argument("target", nargs="?", default=None, help="target site id (auto-pick if omitted)")

    p_drain = sub.add_parser("drain", help="wait for all instances to finish")
    p_drain.add_argument("--timeout", type=float, default=60.0)

    sub.add_parser("shutdown", help="shut the manager + sites down cleanly")

    args = ap.parse_args()
    auth_key = protocol.derive_key(args.auth_key) if args.auth_key else None

    handlers = {
        "status": cmd_status, "topology": cmd_topo, "instances": cmd_instances,
        "migrate": cmd_migrate, "drain": cmd_drain, "shutdown": cmd_shutdown,
    }
    return handlers[args.cmd](args, auth_key)


if __name__ == "__main__":
    sys.exit(main() or 0)
