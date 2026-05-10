#!/usr/bin/env python3
"""Build Pro6000 K=4 summary table from summary.json (W2_phase M8)."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np


BASE = Path(
    "./pro6000_K4_M8/"
    "W2_phase_hol_rps3_p2048_split_M8"
)
OUT = Path("./figures/tab_pro6000_k4_m8.tex")

POLICIES = [
    ("vanilla", "GlobalFIFO"),
    ("gate_rr", "CLIMB"),
]

METRICS = [
    ("vip_ttft_ms", "p99", "VIP TTFT p99 (s)"),
    ("vip_queue_ms", "p99", "VIP queue p99 (s)"),
    ("vip_engine_ms", "p99", "VIP engine p99 (s)"),
    ("throughput_rps", None, "Throughput (rps)"),
    ("bg_ttft_ms", "p99", "BG TTFT p99 (s)"),
]


def load_metric(data: dict, key: str, subkey: str | None) -> float | None:
    if subkey is None:
        return data.get(key)
    return data.get(key, {}).get(subkey)


def fmt(mean: float, std: float) -> str:
    return f"{mean:.2f} $\\pm$ {std:.2f}"


def main() -> None:
    rows = []
    for policy, label in POLICIES:
        vals = {key: [] for key, _, _ in METRICS}
        for seed_dir in sorted(BASE.joinpath(policy).glob("seed=*")):
            data = json.load(open(seed_dir / "summary.json", "r"))
            for key, subkey, _ in METRICS:
                v = load_metric(data, key, subkey)
                if v is None:
                    continue
                vals[key].append(float(v))
        row = [label]
        for key, _, _ in METRICS:
            arr = np.array(vals[key], dtype=float)
            if arr.size == 0:
                row.append("NA")
                continue
            mean = arr.mean()
            std = arr.std(ddof=1) if arr.size > 1 else 0.0
            if key.endswith("_ms"):
                mean /= 1000.0
                std /= 1000.0
            row.append(fmt(mean, std))
        rows.append(row)

    headers = ["Policy"] + [h for _, _, h in METRICS]

    lines = []
    lines.append("\\begin{table}[t]")
    lines.append("\\centering")
    lines.append("\\small")
    lines.append("\\setlength{\\tabcolsep}{4pt}")
    lines.append("\\begin{tabular}{lccccc}")
    lines.append("\\toprule")
    lines.append(" & ".join(headers) + " \\\\")
    lines.append("\\midrule")
    for row in rows:
        lines.append(" & ".join(row) + " \\\\")
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append("\\vspace{2pt}")
    lines.append(
        "\\caption{RTX Pro 6000 (96GB) with 110GB RAM. "
        "Workload: W2\\_phase\\_hol\\_rps3\\_p2048\\_split\\_M8, K=4. "
        "Values are mean $\\pm$ std over seeds (101/102/103), in seconds.}"
    )
    lines.append("\\label{tab:pro6000_k4_m8}")
    lines.append("\\end{table}")
    OUT.write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
