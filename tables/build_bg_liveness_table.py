#!/usr/bin/env python3
"""
Rebuild Table 5.5 (BG liveness) from requests_log.csv using the exp_plan_main.md
backlogged_wait definition.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def percentile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    values = sorted(values)
    if q <= 0:
        return float(values[0])
    if q >= 1:
        return float(values[-1])
    idx = q * (len(values) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(values) - 1)
    if lo == hi:
        return float(values[lo])
    frac = idx - lo
    return float(values[lo] * (1 - frac) + values[hi] * frac)


def weighted_percentile(values: List[float], weights: List[int], q: float) -> Optional[float]:
    if not values or not weights:
        return None
    pairs = sorted(zip(values, weights), key=lambda x: x[0])
    total = float(sum(weights))
    if total <= 0:
        return None
    target = q * total
    acc = 0.0
    for v, w in pairs:
        acc += w
        if acc >= target:
            return float(v)
    return float(pairs[-1][0])


def parse_seed(name: str) -> Optional[int]:
    m = re.search(r"seed=(\d+)", name)
    if not m:
        return None
    return int(m.group(1))


def parse_ts(name: str) -> int:
    m = re.search(r"ts=(\d{8}-\d{6})", name)
    if not m:
        return 0
    return int(m.group(1).replace("-", ""))


def select_runs(policy_dir: Path, seeds: List[int]) -> Dict[int, Path]:
    runs: Dict[int, Tuple[int, Path]] = {}
    for run in policy_dir.glob("seed=*"):
        seed = parse_seed(run.name)
        if seed is None or seed not in seeds:
            continue
        ts = parse_ts(run.name)
        prev = runs.get(seed)
        if prev is None or ts > prev[0]:
            runs[seed] = (ts, run)
    return {seed: run for seed, (_, run) in runs.items()}


def load_events(path: Path) -> Dict[str, List[Tuple[float, int]]]:
    events: Dict[str, List[Tuple[float, int]]] = {}
    with path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ok = str(row.get("ok", "")).strip().lower()
            if ok not in {"true", "1", "yes"}:
                continue
            if row.get("class") != "BG":
                continue
            arrival = row.get("arrival_ts")
            dispatch = row.get("dispatch_ts")
            if not arrival or not dispatch:
                continue
            try:
                arrival_ts = float(arrival)
                dispatch_ts = float(dispatch)
            except ValueError:
                continue
            adapter = row.get("adapter_id")
            if not adapter:
                continue
            evs = events.setdefault(adapter, [])
            evs.append((arrival_ts, 0))   # arrival
            evs.append((dispatch_ts, 1))  # dispatch
    return events


def compute_backlogged_waits(
    events: Dict[str, List[Tuple[float, int]]]
) -> Tuple[Dict[str, List[float]], Dict[str, List[int]]]:
    waits_by_adapter: Dict[str, List[float]] = {}
    weights_by_adapter: Dict[str, List[int]] = {}
    for adapter, evs in events.items():
        evs.sort(key=lambda x: (x[0], x[1]))
        count = 0
        in_episode = False
        served = False
        start_ts = 0.0
        episode_reqs = 0
        waits: List[float] = []
        weights: List[int] = []
        for ts, kind in evs:
            if kind == 0:  # arrival
                if count == 0 and not in_episode:
                    in_episode = True
                    served = False
                    start_ts = ts
                    episode_reqs = 0
                if in_episode and not served:
                    episode_reqs += 1
                count += 1
            else:  # dispatch
                if in_episode and not served:
                    wait_ms = (ts - start_ts) * 1000.0
                    waits.append(wait_ms)
                    weights.append(max(episode_reqs, 1))
                    served = True
                count -= 1
                if count <= 0:
                    count = 0
                    in_episode = False
                    served = False
                    start_ts = 0.0
                    episode_reqs = 0
        if waits:
            waits_by_adapter[adapter] = waits
            weights_by_adapter[adapter] = weights
    return waits_by_adapter, weights_by_adapter


def format_mean_std(mean: float, std: float) -> str:
    if abs(mean) >= 100 or abs(std) >= 100:
        return f"{mean:.1f} $\\pm$ {std:.1f}"
    return f"{mean:.2f} $\\pm$ {std:.2f}"


def compute_seed_metrics(run_dir: Path) -> Tuple[Optional[float], Optional[float]]:
    log_path = run_dir / "requests_log.csv"
    if not log_path.exists():
        return None, None
    events = load_events(log_path)
    waits_by_adapter, weights_by_adapter = compute_backlogged_waits(events)
    per_adapter_p99 = []
    all_waits = []
    all_weights = []
    for adapter, waits in waits_by_adapter.items():
        p99 = percentile(waits, 0.99)
        if p99 is not None:
            per_adapter_p99.append(p99)
        for wait, w in zip(waits, weights_by_adapter.get(adapter, [])):
            all_waits.append(wait)
            all_weights.append(w)
    worst = max(per_adapter_p99) if per_adapter_p99 else None
    overall = weighted_percentile(all_waits, all_weights, 0.99)
    return overall, worst


def mean_std(values: List[float]) -> Tuple[float, float]:
    if not values:
        return float("nan"), float("nan")
    mean = sum(values) / len(values)
    var = sum((v - mean) ** 2 for v in values) / len(values)
    return mean, math.sqrt(var)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--data-root",
        default="./server_pull_hol_E_20260119_165233/hol_liveness_M8_rps1_K4",
    )
    p.add_argument(
        "--workload",
        default="W2_phase_hol_rps1_p2048_split_M8",
    )
    p.add_argument(
        "--policies",
        default="vanilla,gate_rr",
    )
    p.add_argument(
        "--seeds",
        default="101,102,103",
    )
    p.add_argument(
        "--out",
        default="./figures/tab_bg_liveness.tex",
    )
    args = p.parse_args()

    data_root = Path(args.data_root)
    workload_dir = data_root / args.workload
    policies = [p.strip() for p in args.policies.split(",") if p.strip()]
    seeds = [int(s) for s in args.seeds.split(",") if s.strip()]

    rows = []
    for policy in policies:
        policy_dir = workload_dir / policy
        runs = select_runs(policy_dir, seeds)
        overall_vals = []
        worst_vals = []
        missing = [s for s in seeds if s not in runs]
        if missing:
            print(f"[warn] {policy}: missing seeds {missing}", file=sys.stderr)
        for seed, run_dir in runs.items():
            overall, worst = compute_seed_metrics(run_dir)
            if overall is None or worst is None:
                print(f"[warn] {policy}: seed {seed} has incomplete metrics", file=sys.stderr)
                continue
            overall_vals.append(overall)
            worst_vals.append(worst)
        overall_mean, overall_std = mean_std(overall_vals)
        worst_mean, worst_std = mean_std(worst_vals)
        rows.append((policy, overall_mean, overall_std, worst_mean, worst_std))

    policy_label = {
        "vanilla": "\\textsc{GlobalFIFO}",
        "gate_rr": "\\textsc{CLIMB}",
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        f.write("\\begin{table}[t]\n")
        f.write("  \\centering\n")
        f.write("  \\small\n")
        f.write("  \\setlength{\\tabcolsep}{4pt}\n")
        f.write("  \\caption{BG liveness cost via \\texttt{backlogged\\_wait} at a reduced-load liveness probe (W=8, K=4). Values are mean $\\pm$ std over seeds, in ms.}\n")
        f.write("  \\label{tab:bg_liveness}\n")
        f.write("  \\begin{tabular}{lrr}\n")
        f.write("    \\toprule\n")
        f.write("    Policy & Overall p99 (ms) & Worst-adapter p99 (ms) \\\\\n")
        f.write("    \\midrule\n")
        for policy, o_mean, o_std, w_mean, w_std in rows:
            label = policy_label.get(policy, policy)
            if math.isnan(o_mean) or math.isnan(w_mean):
                o_text = "N/A"
                w_text = "N/A"
            else:
                o_text = format_mean_std(o_mean, o_std)
                w_text = format_mean_std(w_mean, w_std)
            f.write(f"    {label} & {o_text} & {w_text} \\\\\n")
        f.write("    \\bottomrule\n")
        f.write("  \\end{tabular}\n")
        f.write("\\end{table}\n")

    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
