#!/usr/bin/env python3
"""
Build tab_per_class_throughput_tok_eq.tex from lightweight paper_data summary.json.
"""
from __future__ import annotations

import argparse
import csv
import json
import statistics
from pathlib import Path
from typing import Dict, List, Tuple


POLICY_MAP = {
    "vanilla": "GlobalFIFO",
    "gate_rr": "CLIMB",
}


def _mean_std(vals: List[float]) -> Tuple[float, float]:
    if not vals:
        return 0.0, 0.0
    if len(vals) == 1:
        return vals[0], 0.0
    return statistics.mean(vals), statistics.stdev(vals)


def _load_runs(root: Path, k: int, policy: str) -> List[Path]:
    return sorted((root / f"ksweep_k{k}" / policy).glob("seed=*/summary.json"))


def _load_csv_runs(csv_path: Path) -> Dict[Tuple[int, str], List[Dict[str, float]]]:
    runs: Dict[Tuple[int, str], List[Dict[str, float]]] = {}
    with csv_path.open() as f:
        r = csv.DictReader(f)
        for row in r:
            k = int(row["k"])
            policy = row["policy"]
            runs.setdefault((k, policy), []).append(
                {
                    "vip": float(row["vip_rps"]),
                    "bg": float(row["bg_rps"]),
                    "total": float(row["total_rps"]),
                    "tokeq": float(row["tokeq_s"]),
                }
            )
    return runs


def _compute_metrics(summary: Dict[str, float]) -> Tuple[float, float, float, float]:
    duration_s = float(summary["duration_s"])
    warmup_s = float(summary.get("warmup_s", 0.0))
    effective = max(0.0, duration_s - warmup_s)
    if effective <= 0:
        return 0.0, 0.0, 0.0, 0.0
    ok_vip = float(summary.get("ok_count_vip", 0.0))
    ok_bg = float(summary.get("ok_count_bg", 0.0))
    thr_vip = ok_vip / effective
    thr_bg = ok_bg / effective
    thr_total = (ok_vip + ok_bg) / effective
    # TokEq/s
    l_vip, l_bg, t = 64.0, 2048.0, 64.0
    tokeq = thr_vip * (l_vip + t) + thr_bg * (l_bg + t)
    return thr_vip, thr_bg, thr_total, tokeq


def _format_pm(mean: float, std: float, decimals: int) -> str:
    return f"{mean:.{decimals}f} $\\pm$ {std:.{decimals}f}"


def build_table(root: Path, output: Path, runs_csv: Path) -> None:
    rows = []
    csv_runs = _load_csv_runs(runs_csv) if runs_csv.exists() else None
    for k in (4, 8):
        for policy in ("GlobalFIFO", "CLIMB"):
            vip_vals, bg_vals, total_vals, tokeq_vals = [], [], [], []
            if csv_runs is not None and (k, policy) in csv_runs:
                for row in csv_runs[(k, policy)]:
                    vip_vals.append(row["vip"])
                    bg_vals.append(row["bg"])
                    total_vals.append(row["total"])
                    tokeq_vals.append(row["tokeq"])
            else:
                # fallback to summary.json layout
                inv_map = {v: k for k, v in POLICY_MAP.items()}
                policy_key = inv_map[policy]
                runs = _load_runs(root, k, policy_key)
                if not runs:
                    raise FileNotFoundError(f"No summary.json under {root}/ksweep_k{k}/{policy_key}")
                for run in runs:
                    with run.open() as f:
                        summary = json.load(f)
                    thr_vip, thr_bg, thr_total, tokeq = _compute_metrics(summary)
                    vip_vals.append(thr_vip)
                    bg_vals.append(thr_bg)
                    total_vals.append(thr_total)
                    tokeq_vals.append(tokeq)

            vip_mean, vip_std = _mean_std(vip_vals)
            bg_mean, bg_std = _mean_std(bg_vals)
            total_mean, total_std = _mean_std(total_vals)
            tokeq_mean, tokeq_std = _mean_std(tokeq_vals)
            rows.append(
                {
                    "k": k,
                    "policy": policy,
                    "vip": _format_pm(vip_mean, vip_std, 3),
                    "bg": _format_pm(bg_mean, bg_std, 3),
                    "total": _format_pm(total_mean, total_std, 3),
                    "tokeq": _format_pm(tokeq_mean, tokeq_std, 1),
                }
            )

    lines = []
    lines.append(r"\begin{table}[t]")
    lines.append(r"\centering")
    lines.append(r"\small")
    lines.append(r"\setlength{\tabcolsep}{6pt}")
    lines.append(
        r"\caption{\textbf{Per-class throughput and token-equivalent throughput} (\texttt{W2\_phase}, M8; mean $\pm$ std over 3 seeds)."
    )
    lines.append(
        r"Per-class rps is computed from completed requests over the post-warmup measurement window used for latency metrics (warmup excluded)."
    )
    lines.append(
        r"TokEq/s is an \emph{equivalent} throughput based on configured token budgets:"
    )
    lines.append(
        r"$\text{TokEq/s}=\mathrm{Thr}_{\textsf{VIP}}\cdot(L_{\text{vip}}{+}T)+\mathrm{Thr}_{\textsf{BG}}\cdot(L_{\text{bg}}{+}T)$,"
    )
    lines.append(
        r"with $L_{\text{vip}}{=}64$, $L_{\text{bg}}{=}2048$, and $T{=}64$; it is not the realized token rate.}"
    )
    lines.append(r"\begin{tabular}{c l c c c c}")
    lines.append(r"\toprule")
    lines.append(
        r"\textbf{$K$} & \textbf{Policy} & \textbf{\textsf{VIP} rps} & \textbf{\textsf{BG} rps} & \textbf{Total rps} & \textbf{TokEq/s} \\"
    )
    lines.append(r"\midrule")

    # Emit rows grouped by K (match paper formatting)
    for k in (4, 8):
        k_rows = [r for r in rows if r["k"] == k]
        lines.append(rf"\multirow{{2}}{{*}}{{{k}}}")
        lines.append(
            rf"& \textsc{{{k_rows[0]['policy']}}} & {k_rows[0]['vip']} & {k_rows[0]['bg']} & {k_rows[0]['total']} & {k_rows[0]['tokeq']} \\"
        )
        lines.append(
            rf"& \textsc{{{k_rows[1]['policy']}}} & {k_rows[1]['vip']} & {k_rows[1]['bg']} & {k_rows[1]['total']} & {k_rows[1]['tokeq']} \\"
        )
        lines.append(r"\midrule" if k == 4 else r"\bottomrule")

    lines.append(r"\end{tabular}")
    lines.append(r"\vspace{2pt}")
    lines.append("")
    lines.append("")
    lines.append(r"\label{tab:per_class_throughput_tok_eq}")
    lines.append(r"\end{table}")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    here = Path(__file__).resolve().parent
    default_root = (here / ".." / "paper_data" / "summary").resolve()
    default_runs_csv = (here / ".." / "paper_data" / "per_class_throughput_runs.csv").resolve()
    default_out = (here / ".." / "results" / "summary" / "tab_per_class_throughput_tok_eq.tex").resolve()
    parser.add_argument("--paper-data", type=Path, default=default_root, help="paper_data/summary root")
    parser.add_argument("--runs-csv", type=Path, default=default_runs_csv, help="per-class throughput runs CSV")
    parser.add_argument("--output", type=Path, default=default_out, help="output .tex path")
    args = parser.parse_args()
    build_table(args.paper_data, args.output, args.runs_csv)


if __name__ == "__main__":
    main()
