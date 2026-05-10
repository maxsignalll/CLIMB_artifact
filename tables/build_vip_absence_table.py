#!/usr/bin/env python3
"""
Build tab_vip_absence.tex from lightweight per-seed metrics CSV.
"""
from __future__ import annotations

import argparse
import csv
import statistics
from pathlib import Path
from typing import Dict, List, Tuple


def _mean_std(vals: List[float]) -> Tuple[float, float]:
    if not vals:
        return 0.0, 0.0
    if len(vals) == 1:
        return vals[0], 0.0
    return statistics.mean(vals), statistics.stdev(vals)


def _fmt(mean: float, std: float, decimals: int) -> str:
    return f"{mean:.{decimals}f}$\\pm${std:.{decimals}f}"


def _fmt_total(mean: float, std: float) -> str:
    dec = 2 if mean >= 1.0 else 3
    return _fmt(mean, std, dec)


def _fmt_share(mean: float, std: float) -> str:
    dec = 5 if mean < 0.01 else 3
    return _fmt(mean, std, dec)


def _fmt_bmax(mean: float, std: float) -> str:
    dec = 1 if mean >= 1.0 else 4
    return _fmt(mean, std, dec)


def _fmt_cmax(mean: float, std: float) -> str:
    dec = 1 if mean >= 1.0 else 3
    return _fmt(mean, std, dec)


def build_table(metrics_csv: Path, output: Path) -> None:
    rows: Dict[str, Dict[str, List[float]]] = {}
    with metrics_csv.open() as f:
        r = csv.DictReader(f)
        for row in r:
            policy = row["policy"]
            rows.setdefault(policy, {k: [] for k in ["b_total_s", "b_share", "b_max_s", "c_total_s", "c_share", "c_max_s"]})
            for key in rows[policy].keys():
                rows[policy][key].append(float(row[key]))

    def summarize(policy: str) -> Dict[str, str]:
        b_total_m, b_total_s = _mean_std(rows[policy]["b_total_s"])
        b_share_m, b_share_s = _mean_std(rows[policy]["b_share"])
        b_max_m, b_max_s = _mean_std(rows[policy]["b_max_s"])
        c_total_m, c_total_s = _mean_std(rows[policy]["c_total_s"])
        c_share_m, c_share_s = _mean_std(rows[policy]["c_share"])
        c_max_m, c_max_s = _mean_std(rows[policy]["c_max_s"])
        return {
            "b_total": _fmt_total(b_total_m, b_total_s),
            "b_share": _fmt_share(b_share_m, b_share_s),
            "b_max": _fmt_bmax(b_max_m, b_max_s),
            "c_total": _fmt_total(c_total_m, c_total_s),
            "c_share": _fmt_share(c_share_m, c_share_s),
            "c_max": _fmt_cmax(c_max_m, c_max_s),
        }

    # Match paper spacing (CLIMB row has two spaces before '&')
    order = [("GlobalFIFO", "\\textsc{GlobalFIFO}"), ("CLIMB", "\\textsc{CLIMB} ")]
    lines = []
    lines.append(r"\begin{table}[h]")
    lines.append(r"\centering")
    lines.append(r"\caption{\textbf{\textsf{VIP}-absence intervals (K=7, W2\_phase, W=8).} Mean$\pm$std over three seeds.}")
    lines.append(r"\label{tab:vip_absence}")
    lines.append(r"\small")
    lines.append(r"\setlength{\tabcolsep}{6pt}")
    lines.append(r"\begin{tabular}{l r r r r r r}")
    lines.append(r"\toprule")
    lines.append(r"Policy & B total (s) & B share & B max (s) & C total (s) & C share & C max (s) \\")
    lines.append(r"\midrule")
    for key, label in order:
        stats = summarize(key)
        lines.append(
            rf"{label} & {stats['b_total']} & {stats['b_share']} & {stats['b_max']} & "
            rf"{stats['c_total']} & {stats['c_share']} & {stats['c_max']} \\"
        )
    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\end{table}")

    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    here = Path(__file__).resolve().parent
    default_metrics = (here / ".." / "paper_data" / "vip_absence_metrics.csv").resolve()
    default_out = (here / ".." / "results" / "summary" / "tab_vip_absence.tex").resolve()
    parser.add_argument("--metrics", type=Path, default=default_metrics, help="per-seed metrics CSV")
    parser.add_argument("--output", type=Path, default=default_out, help="output .tex path")
    args = parser.parse_args()
    build_table(args.metrics, args.output)


if __name__ == "__main__":
    main()
