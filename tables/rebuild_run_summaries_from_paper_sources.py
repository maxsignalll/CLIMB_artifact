#!/usr/bin/env python3
"""
Rebuild paper_data/run_summaries.csv from the paper-time summary.json sources
listed in docs/records/03_产物清单.md.

This script is intentionally narrow: it only includes the table_id groups
used by our result tables (cliff_safe, controls, baseline_zoo, pro6000).
For bg_liveness, we optionally carry over the existing rows because the
backlogged-wait metrics are computed from request logs (not in summary.json).
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKSPACE_ROOT = Path(__file__).resolve().parents[2]

# Paper-time sources (from docs/records/03_产物清单.md)
SOURCES = {
    "cliff_safe": [
        WORKSPACE_ROOT / "server_pull_hol_BDE_20260119_125439" / "hol_Ksweep_M8_k4",
        WORKSPACE_ROOT / "server_pull_hol_B_ksweep_k8_20260119_155128" / "hol_Ksweep_M8_k8",
    ],
    "controls": [
        WORKSPACE_ROOT / "server_pull_hol_BDE_20260119_125439" / "hol_ratio_M4_K2_rps7",
        WORKSPACE_ROOT / "server_pull_FH_3seed_20260120_125210" / "hol_KeqW_M8_k8",
        WORKSPACE_ROOT / "server_pull_FH_3seed_20260120_125210" / "hol_KeqWminus1_M8_k7",
    ],
    "baseline_zoo": [
        WORKSPACE_ROOT / "server_pull_FH_3seed_20260120_125210" / "hol_baseline_zoo_M8_k4",
        WORKSPACE_ROOT / "server_pull_FH_3seed_20260120_125210" / "hol_baseline_zoo_M8_k8",
    ],
    "pro6000": [
        WORKSPACE_ROOT / "pro6000_K4_M8" / "W2_phase_hol_rps3_p2048_split_M8",
    ],
}

POLICIES = {
    "cliff_safe": {"vanilla", "gate_rr"},
    "controls": {"vanilla", "gate_rr"},
    "baseline_zoo": {"vanilla", "gate_rr", "cache_aware", "cap_only", "no_switch"},
    "pro6000": {"vanilla", "gate_rr"},
}

RUN_ID_TS_RE = re.compile(r"ts=([0-9\-]+)")
SEED_RE = re.compile(r"seed=([0-9]+)")
K_RE = re.compile(r"__K=([0-9]+)")


def _extract_ts(run_id: str) -> str:
    m = RUN_ID_TS_RE.search(run_id)
    return m.group(1) if m else ""


def _select_latest(cands: List[Dict[str, str]]) -> Dict[str, str]:
    if len(cands) == 1:
        return cands[0]
    return max(cands, key=lambda r: _extract_ts(r.get("run_id", "")))


def _load_summary(path: Path) -> Dict[str, object]:
    return json.loads(path.read_text())


def _summary_to_row(table_id: str, summary_path: Path) -> Dict[str, str]:
    data = _load_summary(summary_path)

    run_id = str(data.get("run_id", summary_path.parent.name))
    policy = str(data.get("policy", summary_path.parent.parent.name))
    k = str(data.get("k", _infer_k(run_id)))
    seed = str(data.get("seed", _infer_seed(run_id)))
    workload_id = str(data.get("workload_id", _infer_workload_id(summary_path)))

    def _p99(obj_key: str) -> str:
        val = data.get(obj_key, {})
        if isinstance(val, dict):
            p99 = val.get("p99")
            return "" if p99 is None else str(p99)
        return ""

    row = {
        "run_id": run_id,
        "policy": policy,
        "k": k,
        "seed": seed,
        "workload_id": workload_id,
        "table_id": table_id,
        "vip_ttft_p99_ms": _p99("vip_ttft_ms"),
        "vip_queue_p99_ms": _p99("vip_queue_ms"),
        "vip_engine_p99_ms": _p99("vip_engine_ms"),
        "bg_ttft_p99_ms": _p99("bg_ttft_ms"),
        "throughput_rps": str(data.get("throughput_rps", "")),
        "bg_backlogged_wait_p99_ms": "",
        "bg_backlogged_wait_p99_worst_ms": "",
    }
    return row


def _infer_seed(run_id: str) -> str:
    m = SEED_RE.search(run_id)
    return m.group(1) if m else ""


def _infer_k(run_id: str) -> str:
    m = K_RE.search(run_id)
    return m.group(1) if m else ""


def _infer_workload_id(summary_path: Path) -> str:
    # workload_id usually appears as a directory above policy/seed
    parts = summary_path.parts
    # try to find a token that looks like workload (starts with W)
    for token in parts[::-1]:
        if token.startswith("W") and "_" in token:
            return token
    return ""


def _gather_rows(table_id: str, roots: List[Path], allowed_policies: set) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for root in roots:
        if not root.exists():
            continue
        for summary_json in root.rglob("summary.json"):
            row = _summary_to_row(table_id, summary_json)
            if row["policy"] not in allowed_policies:
                continue
            if not row["seed"] or not row["k"]:
                continue
            rows.append(row)
    # Deduplicate by (table_id, policy, k, seed, workload_id)
    dedup: Dict[Tuple[str, str, str, str, str], List[Dict[str, str]]] = {}
    for r in rows:
        key = (r["table_id"], r["policy"], r["k"], r["seed"], r["workload_id"])
        dedup.setdefault(key, []).append(r)
    return [_select_latest(v) for v in dedup.values()]


def _load_existing_bg_liveness(existing_csv: Path) -> List[Dict[str, str]]:
    if not existing_csv.exists():
        return []
    with existing_csv.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return [r for r in reader if r.get("table_id") == "bg_liveness"]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(REPO_ROOT / "paper_data" / "run_summaries.csv"))
    parser.add_argument("--existing", default=str(REPO_ROOT / "paper_data" / "run_summaries.csv"))
    args = parser.parse_args()

    out_csv = Path(args.out)
    existing_csv = Path(args.existing)

    all_rows: List[Dict[str, str]] = []
    for table_id, roots in SOURCES.items():
        rows = _gather_rows(table_id, roots, POLICIES[table_id])
        all_rows.extend(rows)

    # Carry over bg_liveness rows if present (computed from request logs)
    all_rows.extend(_load_existing_bg_liveness(existing_csv))

    # Sort for stability
    all_rows.sort(key=lambda r: (r.get("table_id", ""), r.get("policy", ""), r.get("k", ""), r.get("seed", "")))

    fieldnames = [
        "run_id",
        "policy",
        "k",
        "seed",
        "workload_id",
        "table_id",
        "vip_ttft_p99_ms",
        "vip_queue_p99_ms",
        "vip_engine_p99_ms",
        "bg_ttft_p99_ms",
        "throughput_rps",
        "bg_backlogged_wait_p99_ms",
        "bg_backlogged_wait_p99_worst_ms",
    ]

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_rows)


if __name__ == "__main__":
    main()
