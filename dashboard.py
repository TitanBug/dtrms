#!/usr/bin/env python3
"""
TeleRM HTML dashboard generator (M4 observability).

Reads a results directory produced by `run_demo.py` and emits a single
self-contained `dashboard.html` file (no external dependencies, no
JavaScript CDN). The page contains:

  - blocking-probability bar chart (overall + per-service)
  - per-site CPU/mem/bw utilisation bars
  - link utilisation bars
  - decision-time stats (mean / max / count)
  - migration stats (M3)
  - admission-concurrency + auth + recovery flags (M3 + M4)
  - utilisation time-series line chart (CPU per site over time)

The styling is pure inline CSS so the HTML file is fully portable —
email-attachable, viewable from any browser, no static asset server.

Usage:
    python3 dashboard.py --results results/edge_first-seed42-... --out /tmp/d.html
    # then open /tmp/d.html in a browser, or:
    xdg-open /tmp/d.html    # Linux
    open /tmp/d.html        # macOS
"""
import argparse
import html
import json
import os
import sys
from datetime import datetime


def _load(results_dir):
    paths = {
        "report": "manager_report.json",
        "instances": "instances_log.json",
        "heartbeats": "heartbeat_log.json",
        "utilisation": "utilisation_timeseries.json",
        "final_status": "final_status.json",
        "generator": "generator_summary.json",
        "config": "config_used.json",
    }
    data = {}
    for k, p in paths.items():
        full = os.path.join(results_dir, p)
        if os.path.exists(full):
            try:
                with open(full) as f:
                    data[k] = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                print(f"[dashboard] warning: could not read {p}: {e}", file=sys.stderr)
    return data


def _bar(value_pct, color="#2563eb", height=18, width_pct=None):
    width = max(0.0, min(100.0, float(value_pct if width_pct is None else width_pct)))
    return (f'<div class="bar"><div class="bar-fill" style="width:{width:.1f}%;'
            f'background:{color};height:{height}px"></div></div>')


def _safe(v, fmt="%"):
    if v is None:
        return "n/a"
    return f"{v * 100:.1f}{fmt}" if fmt == "%" else f"{v:{fmt}}"


def _render_blocking(report):
    bp = report.get("blocking_probability", {})
    rows = []
    for k in sorted(k for k in bp if k != "_all"):
        v = bp[k]
        rows.append((k, v))
    out = ['<section class="card">', '<h2>Blocking probability</h2>']
    out.append('<table class="kpi"><tr><th>scope</th><th>offered</th>'
               '<th>blocked</th><th>probability</th><th>bar</th></tr>')
    out.append('<tr class="overall"><td>_all</td>'
               f'<td>{report.get("offered",{}).get("_all",0)}</td>'
               f'<td>{report.get("blocked",{}).get("_all",0)}</td>'
               f'<td>{_safe(bp.get("_all"))}</td>'
               f'<td>{_bar(bp.get("_all",0)*100, "#dc2626")}</td></tr>')
    for k, v in rows:
        out.append(f'<tr><td>{html.escape(k)}</td>'
                   f'<td>{report.get("offered",{}).get(k,0)}</td>'
                   f'<td>{report.get("blocked",{}).get(k,0)}</td>'
                   f'<td>{_safe(v)}</td>'
                   f'<td>{_bar(v*100, "#f59e0b")}</td></tr>')
    out.append('</table>')
    out.append('<details><summary>blocked reasons</summary>')
    out.append('<pre>' + html.escape(json.dumps(report.get("blocked_reasons", {}), indent=2)) + '</pre>')
    out.append('</details>')
    out.append('</section>')
    return "\n".join(out)


def _render_sites(report, final_status):
    sites = report.get("sites_final") or final_status.get("sites", {})
    out = ['<section class="card">', '<h2>Site utilisation (final)</h2>']
    out.append('<table class="kpi"><tr><th>site</th>'
               '<th>cpu alloc/cap</th><th>cpu bar</th>'
               '<th>mem alloc/cap</th><th>mem bar</th>'
               '<th>bw alloc/cap</th><th>bw bar</th></tr>')
    for sid, s in sorted(sites.items()):
        cap = s.get("capacity", {})
        alloc = s.get("allocated", {})
        cpu_p = (alloc.get("cpu", 0) / cap["cpu"] * 100) if cap.get("cpu") else 0
        mem_p = (alloc.get("mem", 0) / cap["mem"] * 100) if cap.get("mem") else 0
        bw_p = (alloc.get("bw", 0) / cap["bw"] * 100) if cap.get("bw") else 0
        out.append(f'<tr><td>{html.escape(sid)}</td>'
                   f'<td>{alloc.get("cpu",0)}/{cap.get("cpu",0)}</td>'
                   f'<td>{_bar(cpu_p, "#0ea5e9")}</td>'
                   f'<td>{alloc.get("mem",0)}/{cap.get("mem",0)}</td>'
                   f'<td>{_bar(mem_p, "#22c55e")}</td>'
                   f'<td>{alloc.get("bw",0)}/{cap.get("bw",0)}</td>'
                   f'<td>{_bar(bw_p, "#a855f7")}</td></tr>')
    out.append('</table></section>')
    return "\n".join(out)


def _render_links(report):
    links = report.get("link_utilisation_final", {})
    out = ['<section class="card">', '<h2>Link utilisation (final)</h2>']
    out.append('<table class="kpi"><tr><th>link</th><th>reserved</th>'
               '<th>capacity</th><th>utilisation</th><th>bar</th></tr>')
    for ln, u in sorted(links.items()):
        out.append(f'<tr><td>{html.escape(ln)}</td>'
                   f'<td>{u.get("reserved",0)}</td>'
                   f'<td>{u.get("capacity",0)}</td>'
                   f'<td>{_safe(u.get("utilisation",0))}</td>'
                   f'<td>{_bar(u.get("utilisation",0)*100, "#6366f1")}</td></tr>')
    out.append('</table></section>')
    return "\n".join(out)


def _render_timeseries(utilisation):
    if not utilisation:
        return '<section class="card"><h2>Utilisation time-series</h2><p>(no data)</p></section>'
    out = ['<section class="card">', '<h2>Utilisation time-series (per-site CPU)</h2>']
    # collect per-site CPU series
    series = {}
    timestamps = []
    for sample in utilisation:
        timestamps.append(sample.get("ts"))
        for sid, m in sample.get("sites", {}).items():
            series.setdefault(sid, []).append(m.get("cpu_util", 0) * 100)
    if not timestamps:
        out.append('<p>(no samples)</p></section>')
        return "\n".join(out)
    n = len(timestamps)
    w = 800
    h = 220
    pad = 30
    inner_w = w - 2 * pad
    inner_h = h - 2 * pad
    colors = ["#0ea5e9", "#22c55e", "#a855f7", "#f59e0b", "#ef4444", "#14b8a6"]
    out.append(f'<svg viewBox="0 0 {w} {h}" class="chart">')
    # axes
    out.append(f'<line x1="{pad}" y1="{pad}" x2="{pad}" y2="{h-pad}" stroke="#555"/>')
    out.append(f'<line x1="{pad}" y1="{h-pad}" x2="{w-pad}" y2="{h-pad}" stroke="#555"/>')
    out.append(f'<text x="{pad-5}" y="{pad-5}" font-size="10" text-anchor="end">100%</text>')
    out.append(f'<text x="{pad-5}" y="{h-pad+5}" font-size="10" text-anchor="end">0%</text>')
    for i, (sid, ys) in enumerate(sorted(series.items())):
        color = colors[i % len(colors)]
        pts = []
        for i2, v in enumerate(ys):
            x = pad + (i2 / max(1, n - 1)) * inner_w
            y = h - pad - (v / 100.0) * inner_h
            pts.append(f"{x:.1f},{y:.1f}")
        out.append(f'<polyline points="{" ".join(pts)}" fill="none" stroke="{color}" stroke-width="2"/>')
        out.append(f'<text x="{w-pad+4}" y="{pad + i*12}" font-size="10" fill="{color}">{html.escape(sid)}</text>')
    out.append('</svg></section>')
    return "\n".join(out)


def _render_kpi_grid(report, generator, final_status):
    mig = report.get("migrations", {})
    dt = report.get("decision_time_ms", {})
    counters = report.get("counters", {})
    out = ['<section class="card kpi-grid">']
    out.append('<div class="kpi-box"><div class="kpi-label">admission</div>'
               f'<div class="kpi-value">{"concurrent" if report.get("admission_concurrent") else "serialised"}</div>'
               '<div class="kpi-hint">M3</div></div>')
    out.append(f'<div class="kpi-box"><div class="kpi-label">auth</div>'
               f'<div class="kpi-value">{"on" if report.get("auth_enabled") else "off"}</div>'
               '<div class="kpi-hint">M4</div></div>')
    out.append(f'<div class="kpi-box"><div class="kpi-label">recovered ops</div>'
               f'<div class="kpi-value">{report.get("recover_applied", 0)}</div>'
               '<div class="kpi-hint">M4</div></div>')
    out.append(f'<div class="kpi-box"><div class="kpi-label">migrations</div>'
               f'<div class="kpi-value">{mig.get("succeeded",0)}/{mig.get("attempted",0)}</div>'
               '<div class="kpi-hint">M3</div></div>')
    out.append(f'<div class="kpi-box"><div class="kpi-label">decision mean</div>'
               f'<div class="kpi-value">{(dt.get("mean") or 0):.2f}ms</div>'
               '<div class="kpi-hint">M3</div></div>')
    out.append(f'<div class="kpi-box"><div class="kpi-label">decision max</div>'
               f'<div class="kpi-value">{(dt.get("max") or 0):.2f}ms</div>'
               '<div class="kpi-hint">M3</div></div>')
    if generator:
        out.append(f'<div class="kpi-box"><div class="kpi-label">requests</div>'
                   f'<div class="kpi-value">{generator.get("requests_sent",0)}</div>'
                   '<div class="kpi-hint">gen</div></div>')
        out.append(f'<div class="kpi-box"><div class="kpi-label">accepted</div>'
                   f'<div class="kpi-value">{generator.get("accepted",0)}</div>'
                   '<div class="kpi-hint">gen</div></div>')
    if counters:
        out.append(f'<div class="kpi-box"><div class="kpi-label">manager msgs</div>'
                   f'<div class="kpi-value">{counters.get("messages_sent",0)+counters.get("messages_recv",0)}</div>'
                   '<div class="kpi-hint">wire</div></div>')
    out.append('</section>')
    return "\n".join(out)


def _render_header(report, config):
    if not config:
        return f'<h1>TeleRM run dashboard</h1>'
    cmd = config.get("command", "")
    started = config.get("started_at", "")
    pyver = config.get("python_version", "").split()[0]
    out = ['<header>']
    out.append('<h1>TeleRM run dashboard</h1>')
    out.append(f'<div class="meta"><span>policy: <code>{html.escape(report.get("policy",""))}</code></span>'
               f'<span>started: <code>{html.escape(started)}</code></span>'
               f'<span>python: <code>{html.escape(pyver)}</code></span></div>')
    out.append(f'<div class="meta"><code>{html.escape(cmd)}</code></div>')
    out.append('</header>')
    return "\n".join(out)


CSS = """
* { box-sizing: border-box; }
body { margin: 0; padding: 24px; font-family: -apple-system, system-ui, 'Segoe UI', Roboto, sans-serif;
       background: #f8fafc; color: #0f172a; }
header { margin-bottom: 24px; padding-bottom: 16px; border-bottom: 2px solid #cbd5e1; }
h1 { margin: 0 0 8px 0; font-size: 26px; font-weight: 700; }
h2 { margin: 0 0 12px 0; font-size: 17px; font-weight: 600; color: #1e293b; }
.meta { font-size: 12px; color: #475569; margin: 4px 0; }
.meta code { background: #e2e8f0; padding: 1px 6px; border-radius: 3px; font-size: 11px; }
.meta span { margin-right: 16px; }
section.card { background: white; border: 1px solid #e2e8f0; border-radius: 8px;
              padding: 16px; margin: 12px 0; box-shadow: 0 1px 3px rgba(0,0,0,0.04); }
.kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(120px, 1fr)); gap: 12px; }
.kpi-box { background: #f1f5f9; border-radius: 6px; padding: 10px; text-align: center; }
.kpi-label { font-size: 10px; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; }
.kpi-value { font-size: 22px; font-weight: 700; color: #0f172a; margin: 4px 0; }
.kpi-hint { font-size: 9px; color: #94a3b8; }
table.kpi { width: 100%; border-collapse: collapse; font-size: 12px; }
table.kpi th { text-align: left; padding: 6px 8px; background: #f1f5f9; border-bottom: 2px solid #cbd5e1; }
table.kpi td { padding: 6px 8px; border-bottom: 1px solid #e2e8f0; }
table.kpi tr.overall { background: #fefce8; font-weight: 600; }
.bar { width: 100%; background: #e2e8f0; border-radius: 3px; min-width: 60px; }
.bar-fill { border-radius: 3px; min-width: 2px; }
svg.chart { width: 100%; height: 220px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; }
details { margin-top: 12px; }
details summary { cursor: pointer; font-size: 12px; color: #475569; }
pre { background: #1e293b; color: #e2e8f0; padding: 12px; border-radius: 6px; overflow: auto; font-size: 11px; }
footer { margin-top: 24px; padding-top: 12px; border-top: 1px solid #cbd5e1;
         font-size: 11px; color: #64748b; }
"""


def render_html(results_dir):
    data = _load(results_dir)
    report = data.get("report", {})
    instances = data.get("instances", [])
    final_status = data.get("final_status", {})
    utilisation = data.get("utilisation", [])
    generator = data.get("generator", {})
    config = data.get("config", {})

    parts = [f"<!doctype html><html><head><meta charset='utf-8'>",
             f"<title>TeleRM dashboard</title>",
             f"<style>{CSS}</style></head><body>"]
    parts.append(_render_header(report, config))
    parts.append(_render_kpi_grid(report, generator, final_status))
    parts.append(_render_blocking(report))
    parts.append(_render_sites(report, final_status))
    parts.append(_render_links(report))
    parts.append(_render_timeseries(utilisation))

    # instances summary table
    if instances:
        parts.append('<section class="card"><h2>Completed instances</h2>')
        parts.append('<table class="kpi"><tr><th>sid</th><th>service</th><th>site</th>'
                     '<th>jobs_done</th><th>busy_s</th><th>holding_s</th>'
                     '<th>exit_reason</th></tr>')
        for inst in instances[:50]:
            parts.append('<tr>'
                         f'<td>{html.escape(str(inst.get("sid","")))}</td>'
                         f'<td>{html.escape(str(inst.get("service","")))}</td>'
                         f'<td>{html.escape(str(inst.get("site","")))}</td>'
                         f'<td>{inst.get("jobs_done","")}</td>'
                         f'<td>{inst.get("busy_s","")}</td>'
                         f'<td>{inst.get("holding_s","")}</td>'
                         f'<td>{html.escape(str(inst.get("summary",{}).get("exit_reason",""))) if isinstance(inst.get("summary"),dict) else html.escape(str(inst.get("exit_reason","")))}</td>'
                         '</tr>')
        parts.append('</table>')
        if len(instances) > 50:
            parts.append(f'<p>(showing 50 of {len(instances)} completed instances)</p>')
        parts.append('</section>')

    parts.append(f'<footer>Generated by <code>dashboard.py</code> at '
                 f'{datetime.now().isoformat()} from <code>{html.escape(results_dir)}</code>'
                 f'<br>M3 = fault tolerance (migration + concurrent admission); '
                 f'M4 = resilience (WAL/snapshot/HMAC) + observability.</footer>')
    parts.append('</body></html>')
    return "\n".join(parts)


def main():
    ap = argparse.ArgumentParser(description="TeleRM HTML dashboard generator (M4)")
    ap.add_argument("--results", required=True, help="results directory from a run_demo.py run")
    ap.add_argument("--out", default=None,
                   help="output HTML path (default: <results>/dashboard.html)")
    args = ap.parse_args()
    if not os.path.isdir(args.results):
        print(f"[dashboard] no such directory: {args.results}", file=sys.stderr)
        return 1
    out_path = args.out or os.path.join(args.results, "dashboard.html")
    html_str = render_html(args.results)
    with open(out_path, "w") as f:
        f.write(html_str)
    print(f"[dashboard] wrote {out_path} ({len(html_str)} bytes)")
    print(f"[dashboard] open it with: xdg-open {out_path}   # Linux")
    return 0


if __name__ == "__main__":
    sys.exit(main())
