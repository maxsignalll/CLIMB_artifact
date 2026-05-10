#!/usr/bin/env python3
import argparse
import json
import math
import statistics
import sys
import time
from collections import deque
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd

from policies.gate_rr import GateRRPolicy

try:
    from ingress.scheduler import RequestItem, Scheduler
except ImportError:
    RequestItem = None
    Scheduler = None


def _safe_mean_std(values: List[float]) -> Tuple[Optional[float], Optional[float]]:
    if not values:
        return None, None
    if len(values) == 1:
        return values[0], 0.0
    return statistics.mean(values), statistics.stdev(values)


def _percentile(values: List[float], pct: float) -> Optional[float]:
    if not values:
        return None
    values = sorted(values)
    idx = int(math.ceil((pct / 100.0) * len(values))) - 1
    idx = max(0, min(idx, len(values) - 1))
    return values[idx]


def _iter_summary_paths(roots: Iterable[Path], policy: str) -> List[Path]:
    paths: List[Path] = []
    for root in roots:
        for path in root.rglob("summary.json"):
            if f"/{policy}/" not in str(path):
                continue
            if "seed=" not in str(path):
                continue
            paths.append(path)
    return sorted(paths)


def _iter_control_paths(roots: Iterable[Path], policy: str) -> List[Path]:
    paths: List[Path] = []
    for root in roots:
        for path in root.rglob("control_log.csv"):
            if f"/{policy}/" not in str(path):
                continue
            if "seed=" not in str(path):
                continue
            paths.append(path)
    return sorted(paths)


def _load_throughput(summary_paths: List[Path]) -> Tuple[Optional[float], Optional[float]]:
    values: List[float] = []
    for path in summary_paths:
        try:
            obj = json.loads(path.read_text())
        except Exception:
            continue
        thr = obj.get("throughput_rps")
        if thr is None:
            thr = obj.get("thr")
        if thr is None:
            thr = obj.get("thr_rps")
        if thr is None:
            continue
        values.append(float(thr))
    return _safe_mean_std(values)


def _load_decision_us(control_paths: List[Path]) -> Tuple[Optional[float], Optional[float]]:
    values: List[float] = []
    for path in control_paths:
        try:
            df = pd.read_csv(path, usecols=["decision_us", "event"])
        except Exception:
            continue
        df = df.dropna(subset=["decision_us"])
        if "event" in df.columns:
            df = df[df["event"] == "dispatch"]
        values.extend(df["decision_us"].astype(float).tolist())
    return _safe_mean_std(values), _percentile(values, 99)


def _switch_rate_stats(control_paths: List[Path], t_start: float, t_end: float) -> Tuple[Optional[float], Optional[float]]:
    rates: List[float] = []
    for path in control_paths:
        try:
            df = pd.read_csv(path, usecols=["ts", "switch_count"])
        except Exception:
            continue
        df = df.dropna(subset=["ts", "switch_count"]).sort_values("ts")
        if df.empty:
            continue
        t0 = df["ts"].iloc[0]
        df["t_rel"] = df["ts"] - t0
        df = df[(df["t_rel"] >= t_start) & (df["t_rel"] <= t_end)]
        if df.empty:
            continue
        df["sec"] = df["t_rel"].astype(int)
        per_sec = df.groupby("sec")["switch_count"].agg(lambda x: x.max() - x.min())
        rates.extend(per_sec.astype(float).tolist())
    return _safe_mean_std(rates), _percentile(rates, 99)


def _load_evict_rate(control_paths: List[Path], t_start: float, t_end: float) -> Tuple[Optional[float], Optional[float]]:
    rates: List[float] = []
    for path in control_paths:
        try:
            df = pd.read_csv(path)
        except Exception:
            continue
        if "load_count" not in df.columns or "evict_count" not in df.columns:
            continue
        df = df.dropna(subset=["ts", "load_count", "evict_count"]).sort_values("ts")
        if df.empty:
            continue
        t0 = df["ts"].iloc[0]
        df["t_rel"] = df["ts"] - t0
        df = df[(df["t_rel"] >= t_start) & (df["t_rel"] <= t_end)]
        if df.empty:
            continue
        df["sec"] = df["t_rel"].astype(int)
        per_sec = df.groupby("sec").apply(
            lambda x: (x["load_count"].max() - x["load_count"].min())
            + (x["evict_count"].max() - x["evict_count"].min())
        )
        rates.extend(per_sec.astype(float).tolist())
    return _safe_mean_std(rates), _percentile(rates, 99)


def _recursive_size(obj, seen: Optional[set] = None) -> int:
    import sys

    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    seen.add(obj_id)
    size = sys.getsizeof(obj)
    if isinstance(obj, dict):
        for k, v in obj.items():
            size += _recursive_size(k, seen)
            size += _recursive_size(v, seen)
    elif isinstance(obj, (list, tuple, set, frozenset, deque)):
        for item in obj:
            size += _recursive_size(item, seen)
    return size


def _measure_policy_overhead(k: int, w: int, ticks: int) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if RequestItem is None or Scheduler is None:
        raise RuntimeError(
            "tables/measure_overhead.py requires the full serving harness "
            "that provides ingress.scheduler. The public artifact uses the "
            "curated overhead snapshot by default."
        )
    policy = GateRRPolicy(k=k, bg_cap=k, cluster_q=4)
    request_log = "/tmp/overhead_request.jsonl"
    control_log = "/tmp/overhead_control.jsonl"
    scheduler = Scheduler(
        policy=policy,
        k=k,
        bg_cap=k,
        cluster_q=4,
        max_inflight=128,
        request_log_path=request_log,
        control_log_path=control_log,
    )
    vip_id = "vip"
    bg_ids = [f"bg{i:02d}" for i in range(1, max(1, w))]
    adapters = [vip_id] + bg_ids[: max(0, w - 1)]
    scheduler.adapter_order = adapters
    scheduler.adapter_class = {vip_id: "VIP"}
    for bg in adapters[1:]:
        scheduler.adapter_class[bg] = "BG"
    scheduler.queues = {}
    now = 0.0
    for adapter_id in adapters:
        req = RequestItem(
            request_id=f"req-{adapter_id}",
            adapter_id=adapter_id,
            cls=scheduler.adapter_class[adapter_id],
            arrival_ts=now,
            payload={},
        )
        scheduler.queues[adapter_id] = deque([req])
    scheduler.active_vip = {vip_id}
    scheduler.active_bg = set(adapters[1:1 + max(0, k - 1)])

    # Warmup
    for _ in range(1000):
        policy.update_active_sets(now)
        policy.pick_next_adapter(now)

    times_us: List[float] = []
    start = time.perf_counter()
    for _ in range(ticks):
        t0 = time.perf_counter()
        policy.update_active_sets(now)
        policy.pick_next_adapter(now)
        times_us.append((time.perf_counter() - t0) * 1_000_000.0)
    elapsed = time.perf_counter() - start
    mean_us = statistics.mean(times_us) if times_us else None
    p99_us = _percentile(times_us, 99)

    try:
        from pympler import asizeof

        size_bytes = asizeof.asizeof(scheduler)
    except Exception:
        size_bytes = _recursive_size(scheduler)

    size_kb = size_bytes / 1024.0
    if mean_us is not None:
        print(f"[microbench] {ticks} ticks in {elapsed:.2f}s, mean={mean_us:.2f}us, p99={p99_us:.2f}us, size={size_kb:.1f}KB")
    return mean_us, p99_us, size_kb


def _format_mean_std(mean: Optional[float], std: Optional[float], fmt: str = "{:.2f}") -> str:
    if mean is None:
        return "N/A"
    if std is None:
        return fmt.format(mean)
    return f"{fmt.format(mean)} $\\pm$ {fmt.format(std)}"


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure controller overhead and update Table 5.4.")
    parser.add_argument("--safe-run", action="append", required=True, help="Safe run root (K>=W). Repeatable.")
    parser.add_argument("--cliff-run", action="append", required=True, help="Cliff run root (K<W). Repeatable.")
    parser.add_argument("--policy", default="gate_rr")
    parser.add_argument("--ticks", type=int, default=1_000_000)
    parser.add_argument("--k", type=int, default=8)
    parser.add_argument("--w", type=int, default=8)
    parser.add_argument("--out-main", default="figures/tab_overhead_main.tex")
    parser.add_argument("--out-appendix", default="figures/tab_overhead_appendix.tex")
    args = parser.parse_args()

    safe_roots = [Path(p) for p in args.safe_run]
    cliff_roots = [Path(p) for p in args.cliff_run]

    # Throughput at safe anchor.
    thr_v_mean, thr_v_std = _load_throughput(_iter_summary_paths(safe_roots, "vanilla"))
    thr_c_mean, thr_c_std = _load_throughput(_iter_summary_paths(safe_roots, args.policy))

    # Control-path overhead (microbench).
    ctrl_mean, ctrl_p99, ctrl_kb = _measure_policy_overhead(args.k, args.w, args.ticks)

    # Decision time from logs (optional sanity).
    decision_mean_std, decision_p99 = _load_decision_us(_iter_control_paths(safe_roots, args.policy))

    # Movement overhead from cliff run.
    switch_mean_std, switch_p99 = _switch_rate_stats(
        _iter_control_paths(cliff_roots, args.policy), t_start=200, t_end=400
    )
    load_mean_std, load_p99 = _load_evict_rate(
        _iter_control_paths(cliff_roots, args.policy), t_start=200, t_end=400
    )

    ctrl_time_str = "N/A"
    if ctrl_mean is not None and ctrl_p99 is not None:
        ctrl_time_str = f"{ctrl_mean:.2f} (p99 {ctrl_p99:.2f})"

    ctrl_state_str = "N/A"
    if ctrl_kb is not None:
        ctrl_state_str = f"{ctrl_kb:.1f}"

    thr_v_str = _format_mean_std(thr_v_mean, thr_v_std)
    thr_c_str = _format_mean_std(thr_c_mean, thr_c_std)

    out_main = Path(args.out_main)
    out_main.write_text(
        "\\begin{table}[t]\n"
        "  \\centering\n"
        "  \\small\n"
        "  \\setlength{\\tabcolsep}{4pt}\n"
        "  \\caption{\\textbf{Controller overhead at safe anchor ($W=8, K=8$).} "
        "Control-path time is measured via a microbenchmark; throughput is from summary logs (mean $\\pm$ std over seeds).}\n"
        "  \\label{tab:overhead_main}\n"
        "  \\begin{tabular}{lrrr}\n"
        "    \\toprule\n"
        "    Policy & Ctrl time/tick (us) & Ctrl state (KB) & Thr@safe (rps) \\\\\n"
        "    \\midrule\n"
        f"    vanilla & N/A & N/A & {thr_v_str} \\\\\n"
        f"    CLIMB (gate\\_rr) & {ctrl_time_str} & {ctrl_state_str} & {thr_c_str} \\\\\n"
        "    \\bottomrule\n"
        "  \\end{tabular}\n"
        "\\end{table}\n"
    )

    switch_mean_str = _format_mean_std(*(switch_mean_std))
    switch_p99_str = f"{switch_p99:.2f}" if switch_p99 is not None else "N/A"
    load_mean_str = _format_mean_std(*(load_mean_std))

    out_appendix = Path(args.out_appendix)
    out_appendix.write_text(
        "\\begin{table*}[t]\n"
        "  \\centering\n"
        "  \\small\n"
        "  \\setlength{\\tabcolsep}{3pt}\n"
        "  \\caption{\\textbf{Controller overhead (appendix).} "
        "Detailed overhead and movement statistics. Units: microseconds (us), kilobytes (KB), and events per second (/s).}\n"
        "  \\label{tab:overhead_ext}\n"
        "  \\begin{tabular}{lrrrrrr}\n"
        "    \\toprule\n"
        "    Policy & Ctrl time mean (us) & Ctrl time p99 (us) & Ctrl state (KB) & Scaling over $K$ & Switch rate avg/p99 (/s) & Load+evict avg (/s) \\\\\n"
        "    \\midrule\n"
        "    vanilla & N/A & N/A & N/A & N/A & N/A & N/A \\\\\n"
        f"    CLIMB (gate\\_rr) & {ctrl_mean:.2f} & {ctrl_p99:.2f} & {ctrl_state_str} & N/A & {switch_mean_str} / {switch_p99_str} & {load_mean_str} \\\\\n"
        "    \\bottomrule\n"
        "  \\end{tabular}\n"
        "\\end{table*}\n"
    )

    print(f"Wrote {out_main}")
    print(f"Wrote {out_appendix}")
    if decision_mean_std[0] is not None:
        print(f"[log] decision_us mean/std: {decision_mean_std}, p99={decision_p99:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
