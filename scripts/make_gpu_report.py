#!/usr/bin/env python3
"""Build a lightweight Markdown report from run_experiment.py outputs.

Inputs: a text file listing summary.json paths (one per line).
Outputs: a Markdown report with key metrics and run paths.
"""

import argparse
import json
from pathlib import Path
from datetime import datetime


def load_summary(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def fmt(x, ndigits=2):
    if x is None:
        return "NA"
    return f"{x:.{ndigits}f}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-list", required=True, help="Text file with summary.json paths")
    ap.add_argument("--output", required=True, help="Output Markdown path")
    ap.add_argument("--exp-name", required=True)
    ap.add_argument("--workload-id", required=True)
    ap.add_argument("--run-tag", default="")
    args = ap.parse_args()

    run_list = Path(args.run_list)
    out_path = Path(args.output)
    if not run_list.exists():
        raise SystemExit(f"run list not found: {run_list}")

    rows = []
    paths = [Path(p.strip()) for p in run_list.read_text().splitlines() if p.strip()]
    for p in paths:
        if not p.exists():
            rows.append({"path": str(p), "error": "missing summary.json"})
            continue
        s = load_summary(p)
        rows.append(
            {
                "path": str(p),
                "policy": s.get("policy_id") or s.get("policy"),
                "k": s.get("k"),
                "seed": s.get("seed"),
                "vip_ttft_p99_ms": s.get("vip_ttft_ms", {}).get("p99"),
                "vip_queue_p99_ms": s.get("vip_queue_ms", {}).get("p99"),
                "vip_engine_p99_ms": s.get("vip_engine_ms", {}).get("p99"),
                "throughput_rps": s.get("throughput_rps"),
                "duration_s": s.get("duration_s"),
            }
        )

    # sort for stable display
    rows.sort(key=lambda r: (r.get("k") or 0, r.get("policy") or "", r.get("seed") or 0))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines = []
    lines.append(f"# GPU run report ({args.exp_name})")
    lines.append("")
    lines.append(f"- Workload: `{args.workload_id}`")
    lines.append(f"- Run tag: `{args.run_tag}`" if args.run_tag else "- Run tag: (not set)")
    lines.append(f"- Generated: {now}")
    lines.append("")
    lines.append("## Summary table")
    lines.append("")
    lines.append("| K | Policy | Seed | VIP TTFT p99 (ms) | VIP queue p99 (ms) | VIP engine p99 (ms) | Throughput (rps) | Duration (s) |")
    lines.append("|---:|---|---:|---:|---:|---:|---:|---:|")
    for r in rows:
        if "error" in r:
            lines.append(f"| NA | NA | NA | NA | NA | NA | NA | NA |  **ERROR**: {r['error']} ({r['path']}) |")
            continue
        lines.append(
            "| {k} | {policy} | {seed} | {ttft} | {queue} | {engine} | {thr} | {dur} |".format(
                k=r.get("k", "NA"),
                policy=r.get("policy", "NA"),
                seed=r.get("seed", "NA"),
                ttft=fmt(r.get("vip_ttft_p99_ms")),
                queue=fmt(r.get("vip_queue_p99_ms")),
                engine=fmt(r.get("vip_engine_p99_ms")),
                thr=fmt(r.get("throughput_rps"), ndigits=3),
                dur=fmt(r.get("duration_s"), ndigits=1),
            )
        )

    lines.append("")
    lines.append("## Run paths")
    lines.append("")
    for r in rows:
        lines.append(f"- {r['path']}")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out_path.as_posix()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
