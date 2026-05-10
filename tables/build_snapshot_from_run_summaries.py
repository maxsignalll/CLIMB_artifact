#!/usr/bin/env python3
"""
Build results_snapshot/tables_snapshot.json from paper_data/run_summaries.csv.

Notes:
- For tables supported by run_summaries, we compute values directly from it.
- For other tables, we fall back to curated TeX under paper_data/tables
  to keep the bundle complete and lightweight.
"""
from __future__ import annotations

import csv
import json
import statistics
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


POLICY_TO_PAPER = {
    "vanilla": "GlobalFIFO",
    "gate_rr": "CLIMB",
    "cache_aware": "LRUGate",
    "cap_only": "BGCap",
    "no_switch": "LockGate",
}


def _mean_std_sample(vals: List[float]) -> Tuple[float, float]:
    if not vals:
        return 0.0, 0.0
    if len(vals) == 1:
        return vals[0], 0.0
    return statistics.mean(vals), statistics.stdev(vals)


def _mean_std_pop(vals: List[float]) -> Tuple[float, float]:
    if not vals:
        return 0.0, 0.0
    if len(vals) == 1:
        return vals[0], 0.0
    return statistics.mean(vals), statistics.pstdev(vals)


def _format_pm(mean: float, std: float, mean_dec: int, std_dec: int) -> str:
    return f"{mean:.{mean_dec}f}$\\pm${std:.{std_dec}f}"


def _format_pm_spaced(mean: float, std: float, mean_dec: int, std_dec: int) -> str:
    return f"{mean:.{mean_dec}f} $\\pm$ {std:.{std_dec}f}"


def _load_run_summaries(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def _write_run_summaries_from_json(summary_root: Path, out_csv: Path) -> None:
    rows = []
    for summary_json in summary_root.rglob("summary.json"):
        data = json.loads(summary_json.read_text())
        row = {
            "run_id": str(data.get("run_id", "")),
            "policy": str(data.get("policy", "")),
            "k": str(data.get("k", "")),
            "seed": str(data.get("seed", "")),
            "workload_id": str(data.get("workload_id", "")),
            "vip_ttft_p99_ms": str(data.get("vip_ttft_ms", {}).get("p99", "")),
            "vip_queue_p99_ms": str(data.get("vip_queue_ms", {}).get("p99", "")),
            "vip_engine_p99_ms": str(data.get("vip_engine_ms", {}).get("p99", "")),
            "bg_ttft_p99_ms": str(data.get("bg_ttft_ms", {}).get("p99", "")),
            "throughput_rps": str(data.get("throughput_rps", "")),
        }
        rows.append(row)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "run_id",
        "policy",
        "k",
        "seed",
        "workload_id",
        "vip_ttft_p99_ms",
        "vip_queue_p99_ms",
        "vip_engine_p99_ms",
        "bg_ttft_p99_ms",
        "throughput_rps",
    ]
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _build_tab_cliff_safe(rows: List[Dict[str, str]]) -> Dict[str, List[str]]:
    # Prefer table_id when present; otherwise fall back to workload_id.
    if any(r.get("table_id") for r in rows):
        rows = [r for r in rows if r.get("table_id") == "cliff_safe"]
    else:
        w2_rows = [r for r in rows if r.get("workload_id")]
        if w2_rows:
            rows = [r for r in rows if r.get("workload_id") == w2_rows[0]["workload_id"]]

    tab_lines: List[str] = []
    tab_lines.append(r"\begin{table}[t]")
    tab_lines.append(r"  \centering")
    tab_lines.append(r"  \scriptsize")
    tab_lines.append(r"  \setlength{\tabcolsep}{2pt}")
    tab_lines.append(
        r"  \caption{\textbf{Cliff vs.\ Safe regimes at fixed $W{=}8$.}"
    )
    tab_lines.append(
        r"  \textsf{VIP} TTFT p99 decomposition and throughput (open-loop)."
    )
    tab_lines.append(
        r"  Values are mean$\pm$std (3 seeds). Units are seconds (s) except throughput (rps)."
    )
    tab_lines.append(
        r"  Queue/Engine columns report component-wise p99 and need not sum to Total p99.}"
    )
    tab_lines.append(r"  \label{tab:cliff_safe}")
    tab_lines.append(r"  \begin{tabular}{l c r r r c}")
    tab_lines.append(r"    \toprule")
    tab_lines.append(r"    \textbf{Policy} & \textbf{$K$} &")
    tab_lines.append(r"    \multicolumn{3}{c}{\textbf{\textsf{VIP} TTFT p99}} &")
    tab_lines.append(r"    \textbf{Thr} \\")
    tab_lines.append(r"    \cmidrule(lr){3-5}")
    tab_lines.append(r"     &  & Total (s) & Queue (s) & Engine (s) & (rps) \\")
    tab_lines.append(r"    \midrule")

    for policy in ("vanilla", "gate_rr"):
        for k in (4, 8):
            sel = [r for r in rows if r.get("policy") == policy and str(r.get("k")) == str(k)]
            if not sel:
                continue
            vip_ttft = [float(r["vip_ttft_p99_ms"]) / 1000.0 for r in sel if r.get("vip_ttft_p99_ms")]
            vip_queue = [float(r["vip_queue_p99_ms"]) / 1000.0 for r in sel if r.get("vip_queue_p99_ms")]
            vip_engine = [float(r["vip_engine_p99_ms"]) / 1000.0 for r in sel if r.get("vip_engine_p99_ms")]
            thr = [float(r["throughput_rps"]) for r in sel if r.get("throughput_rps")]

            ttft_mean, ttft_std = _mean_std_sample(vip_ttft)
            q_mean, q_std = _mean_std_sample(vip_queue)
            e_mean, e_std = _mean_std_sample(vip_engine)
            t_mean, t_std = _mean_std_sample(thr)

            tab_lines.append(
                rf"    \textsc{{{POLICY_TO_PAPER[policy]}}} & {k} & "
                rf"{_format_pm(ttft_mean, ttft_std, 2, 1)} & "
                rf"{_format_pm(q_mean, q_std, 2, 1)} & "
                rf"{_format_pm(e_mean, e_std, 2, 1)} & "
                rf"{_format_pm(t_mean, t_std, 2, 1)} \\")

    tab_lines.append(r"    \bottomrule")
    tab_lines.append(r"  \end{tabular}")
    tab_lines.append(r"\end{table}")

    begin_idx = tab_lines.index(r"  \begin{tabular}{l c r r r c}")
    end_idx = tab_lines.index(r"  \end{tabular}")
    return {
        "source": "run_summaries",
        "pre_lines": tab_lines[:begin_idx],
        "tabular_begin": tab_lines[begin_idx],
        "tabular_lines": tab_lines[begin_idx + 1:end_idx],
        "tabular_end": tab_lines[end_idx],
        "post_lines": tab_lines[end_idx + 1:],
    }


def _snapshot_from_curated(tex_path: Path) -> Dict[str, List[str]]:
    lines = tex_path.read_text().splitlines()
    return _snapshot_from_lines(lines, source="curated_tex")


def _snapshot_from_lines(lines: List[str], source: str) -> Dict[str, List[str]]:
    begin_idx = None
    end_idx = None
    for i, line in enumerate(lines):
        if "\\begin{tabular" in line:
            begin_idx = i
            break
    if begin_idx is None:
        raise RuntimeError("No tabular env in provided lines")
    for i in range(begin_idx + 1, len(lines)):
        if "\\end{tabular" in lines[i]:
            end_idx = i
            break
    if end_idx is None:
        raise RuntimeError("No end tabular env in provided lines")
    return {
        "source": source,
        "pre_lines": lines[:begin_idx],
        "tabular_begin": lines[begin_idx],
        "tabular_lines": lines[begin_idx + 1:end_idx],
        "tabular_end": lines[end_idx],
        "post_lines": lines[end_idx + 1:],
    }


def _table_rows(rows: Iterable[Dict[str, str]], policy: str, k: str) -> List[Dict[str, str]]:
    return [r for r in rows if r.get("policy") == policy and str(r.get("k")) == str(k)]


def _build_tab_baseline_zoo(rows: List[Dict[str, str]]) -> Dict[str, List[str]]:
    baseline_rows = [r for r in rows if r.get("table_id") == "baseline_zoo"]
    tab: List[str] = []
    tab.append(r"\begin{table}[h]")
    tab.append(r"  \centering")
    tab.append(r"  \small")
    tab.append(r"  \setlength{\tabcolsep}{5pt}")
    tab.append(
        r"  \caption{\textbf{Detailed Baseline Performance ($W{=}8$).} "
    )
    tab.append(
        r"  Detailed comparison of all baselines under Cliff ($K{=}4$) and Safe ($K{=}8$) regimes. "
    )
    tab.append(
        r"  \textsc{BGCap} suffers from high \textsf{BG} latency; \textsc{LockGate} suffers from low throughput. "
    )
    tab.append(
        r"  \textsc{CLIMB} provides a strong throughput-preserving trade-off. Values are mean$\pm$std (3 seeds).}"
    )
    tab.append(r"  \label{tab:baseline_zoo}")
    tab.append(r"  \begin{tabular}{l rrr r r}")
    tab.append(r"    \toprule")
    tab.append(r"    & \multicolumn{3}{c}{\textbf{\textsf{VIP} Latency p99 (s)}} & \textbf{\textsf{BG} p99} & \textbf{Thr} \\")
    tab.append(r"    \cmidrule(lr){2-4} \cmidrule(lr){5-5} \cmidrule(lr){6-6}")
    tab.append(r"    \textbf{Policy} & Total (TTFT) & Queue & Engine & (s) & (rps) \\")
    tab.append(r"    \midrule")
    tab.append(r"    \multicolumn{6}{l}{\textit{\textbf{Regime: Cliff} ($K{=}4$)}} \\")

    order = ["vanilla", "cache_aware", "cap_only", "no_switch", "gate_rr"]
    for policy in order:
        sel = _table_rows(baseline_rows, policy, "4")
        std_fn = _mean_std_sample
        if not sel:
            continue
        vip_ttft = [float(r["vip_ttft_p99_ms"]) / 1000.0 for r in sel if r.get("vip_ttft_p99_ms")]
        vip_q = [float(r["vip_queue_p99_ms"]) / 1000.0 for r in sel if r.get("vip_queue_p99_ms")]
        vip_e = [float(r["vip_engine_p99_ms"]) / 1000.0 for r in sel if r.get("vip_engine_p99_ms")]
        bg = [float(r["bg_ttft_p99_ms"]) / 1000.0 for r in sel if r.get("bg_ttft_p99_ms")]
        thr = [float(r["throughput_rps"]) for r in sel if r.get("throughput_rps")]
        ttft_mean, ttft_std = std_fn(vip_ttft)
        q_mean, q_std = std_fn(vip_q)
        e_mean, e_std = std_fn(vip_e)
        bg_mean, bg_std = std_fn(bg)
        t_mean, t_std = std_fn(thr)
        tab.append(
            rf"    \textsc{{{POLICY_TO_PAPER[policy]}}}      & "
            rf"{_format_pm(ttft_mean, ttft_std, 2, 1)} & "
            rf"{_format_pm(q_mean, q_std, 2, 1)} & "
            rf"{_format_pm(e_mean, e_std, 2, 1)} & "
            rf"{_format_pm(bg_mean, bg_std, 2, 1)} & "
            rf"{_format_pm(t_mean, t_std, 2, 1)} \\")

    tab.append(r"    \midrule")
    tab.append(r"    \multicolumn{6}{l}{\textit{\textbf{Regime: Safe Anchor} ($K{=}8$)}} \\")
    for policy in order:
        sel = _table_rows(baseline_rows, policy, "8")
        std_fn = _mean_std_sample
        if not sel:
            continue
        vip_ttft = [float(r["vip_ttft_p99_ms"]) / 1000.0 for r in sel if r.get("vip_ttft_p99_ms")]
        vip_q = [float(r["vip_queue_p99_ms"]) / 1000.0 for r in sel if r.get("vip_queue_p99_ms")]
        vip_e = [float(r["vip_engine_p99_ms"]) / 1000.0 for r in sel if r.get("vip_engine_p99_ms")]
        bg = [float(r["bg_ttft_p99_ms"]) / 1000.0 for r in sel if r.get("bg_ttft_p99_ms")]
        thr = [float(r["throughput_rps"]) for r in sel if r.get("throughput_rps")]
        ttft_mean, ttft_std = std_fn(vip_ttft)
        q_mean, q_std = std_fn(vip_q)
        e_mean, e_std = std_fn(vip_e)
        bg_mean, bg_std = std_fn(bg)
        t_mean, t_std = std_fn(thr)
        tab.append(
            rf"    \textsc{{{POLICY_TO_PAPER[policy]}}}      & "
            rf"{_format_pm(ttft_mean, ttft_std, 2, 1)} & "
            rf"{_format_pm(q_mean, q_std, 2, 1)} & "
            rf"{_format_pm(e_mean, e_std, 2, 1)} & "
            rf"{_format_pm(bg_mean, bg_std, 2, 1)} & "
            rf"{_format_pm(t_mean, t_std, 2, 1)} \\")

    tab.append(r"    \bottomrule")
    tab.append(r"  \end{tabular}")
    tab.append(r"\end{table}")

    return _snapshot_from_lines(tab, source="run_summaries")


def _build_tab_controls(rows: List[Dict[str, str]]) -> Dict[str, List[str]]:
    controls_rows = [r for r in rows if r.get("table_id") == "controls"]
    order = [
        ("D (load-matched)", "M8, K{=}4", "(ref.)", "4"),
        ("D (load-matched)", "M4, K{=}2", "(matched)", "2"),
        ("H (same arrivals)", "K{=}8", "", "8"),
        ("H (same arrivals)", "K{=}7", "", "7"),
    ]

    tab: List[str] = []
    tab.append(r"\begin{table}[t]")
    tab.append(r"  \centering")
    tab.append(r"  \caption{\textbf{Controls (D/H).}")
    tab.append(r"  \textsf{VIP} TTFT p99 and its queue/engine components plus throughput.")
    tab.append(r"  Values are mean$\pm$std over seeds 101/102/103. Units are seconds (s) except throughput (rps).}")
    tab.append(r"  \label{tab:controls}")
    tab.append(r"  \small")
    tab.append(r"  \setlength{\tabcolsep}{5pt}")
    tab.append(r"  \begin{tabular}{llrrrr}")
    tab.append(r"    \toprule")
    tab.append(r"    Control & Setting & \textsf{VIP} TTFT p99 & \textsf{VIP} q p99 & \textsf{VIP} eng p99 & Thr \\")
    tab.append(r"    \midrule")

    for idx, (ctrl, setting, suffix, k) in enumerate(order):
        sel = [r for r in controls_rows if str(r.get("k")) == str(k)]
        if any(r.get("policy") == "vanilla" for r in sel):
            sel = [r for r in sel if r.get("policy") == "vanilla"]
        std_fn = _mean_std_sample
        vip_ttft = [float(r["vip_ttft_p99_ms"]) / 1000.0 for r in sel if r.get("vip_ttft_p99_ms")]
        vip_q = [float(r["vip_queue_p99_ms"]) / 1000.0 for r in sel if r.get("vip_queue_p99_ms")]
        vip_e = [float(r["vip_engine_p99_ms"]) / 1000.0 for r in sel if r.get("vip_engine_p99_ms")]
        thr = [float(r["throughput_rps"]) for r in sel if r.get("throughput_rps")]
        ttft_mean, ttft_std = std_fn(vip_ttft)
        q_mean, q_std = std_fn(vip_q)
        e_mean, e_std = std_fn(vip_e)
        t_mean, t_std = std_fn(thr)

        setting_str = f"${setting}$"
        if suffix:
            setting_str = f"{setting_str} {suffix}"

        tab.append(
            rf"    {ctrl} & {setting_str} & "
            rf"{_format_pm(ttft_mean, ttft_std, 2, 2)} & "
            rf"{_format_pm(q_mean, q_std, 2, 2)} & "
            rf"{_format_pm(e_mean, e_std, 2, 2)} & "
            rf"{_format_pm(t_mean, t_std, 2, 2)} \\")
        if idx == 1:
            tab.append(r"    \midrule")

    tab.append(r"    \bottomrule")
    tab.append(r"  \end{tabular}")
    tab.append(r"\end{table}")

    return _snapshot_from_lines(tab, source="run_summaries")


def _build_tab_pro6000(rows: List[Dict[str, str]]) -> Dict[str, List[str]]:
    rows = [r for r in rows if r.get("table_id") == "pro6000"]
    tab: List[str] = []
    tab.append(r"\begin{table}[t]")
    tab.append(r"\centering")
    tab.append(r"\small")
    tab.append(r"\setlength{\tabcolsep}{4pt}")
    tab.append(r"\caption{RTX PRO 6000 (96GB), RAM 110GB. Workload: \texttt{W2\_phase\_hol\_rps3\_p2048\_split\_M8}, K=4, seeds 101/102/103. Mean $\pm$ std. (seconds; throughput in rps).}")
    tab.append(r"\begin{tabular}{lccccc}")
    tab.append(r"\toprule")
    tab.append(r"Policy & \textsf{VIP} TTFT p99 (s) & \textsf{VIP} queue p99 (s) &  engine p99 (s) & Throughput (rps) & \textsf{BG} TTFT p99 (s) \\")
    tab.append(r"\midrule")
    for policy in ("vanilla", "gate_rr"):
        sel = _table_rows(rows, policy, "4")
        if not sel:
            continue
        vip_ttft = [float(r["vip_ttft_p99_ms"]) / 1000.0 for r in sel if r.get("vip_ttft_p99_ms")]
        vip_q = [float(r["vip_queue_p99_ms"]) / 1000.0 for r in sel if r.get("vip_queue_p99_ms")]
        vip_e = [float(r["vip_engine_p99_ms"]) / 1000.0 for r in sel if r.get("vip_engine_p99_ms")]
        bg = [float(r["bg_ttft_p99_ms"]) / 1000.0 for r in sel if r.get("bg_ttft_p99_ms")]
        thr = [float(r["throughput_rps"]) for r in sel if r.get("throughput_rps")]
        ttft_mean, ttft_std = _mean_std_sample(vip_ttft)
        q_mean, q_std = _mean_std_sample(vip_q)
        e_mean, e_std = _mean_std_sample(vip_e)
        bg_mean, bg_std = _mean_std_sample(bg)
        t_mean, t_std = _mean_std_sample(thr)
        tab.append(
            rf"\textsc{{{POLICY_TO_PAPER[policy]}}} & "
            rf"{_format_pm_spaced(ttft_mean, ttft_std, 2, 2)} & "
            rf"{_format_pm_spaced(q_mean, q_std, 2, 2)} & "
            rf"{_format_pm_spaced(e_mean, e_std, 2, 2)} & "
            rf"{_format_pm_spaced(t_mean, t_std, 2, 2)} & "
            rf"{_format_pm_spaced(bg_mean, bg_std, 2, 2)} \\")
    tab.append(r"\bottomrule")
    tab.append(r"\end{tabular}")
    tab.append(r"\vspace{2pt}")
    tab.append(r"\label{tab:pro6000_k4_m8}")
    tab.append(r"\end{table}")

    return _snapshot_from_lines(tab, source="run_summaries")


def _build_tab_bg_liveness(rows: List[Dict[str, str]]) -> Dict[str, List[str]]:
    rows = [r for r in rows if r.get("table_id") == "bg_liveness"]
    tab: List[str] = []
    tab.append(r"\begin{table}[t]")
    tab.append(r"  \centering")
    tab.append(r"  \small")
    tab.append(r"  \setlength{\tabcolsep}{4pt}")
    tab.append(r"  \caption{BG liveness cost via \texttt{backlogged\_wait} at a reduced-load liveness probe (W=8, K=4). Values are mean $\pm$ std over seeds, in ms.}")
    tab.append(r"  \label{tab:bg_liveness}")
    tab.append(r"  \begin{tabular}{lrr}")
    tab.append(r"    \toprule")
    tab.append(r"    Policy & Overall p99 (ms) & Worst-adapter p99 (ms) \\")
    tab.append(r"    \midrule")
    for policy in ("vanilla", "gate_rr"):
        sel = _table_rows(rows, policy, "4")
        if not sel:
            continue
        overall = [float(r["bg_backlogged_wait_p99_ms"]) for r in sel if r.get("bg_backlogged_wait_p99_ms")]
        worst = [float(r["bg_backlogged_wait_p99_worst_ms"]) for r in sel if r.get("bg_backlogged_wait_p99_worst_ms")]
        o_mean, o_std = _mean_std_sample(overall)
        w_mean, w_std = _mean_std_sample(worst)
        o_dec = 1 if o_mean >= 1000 else 2
        w_dec = 1 if w_mean >= 1000 else 2
        o_std_dec = 1 if o_mean >= 1000 else 2
        w_std_dec = 1 if w_mean >= 1000 else 2
        tab.append(
            rf"    \textsc{{{POLICY_TO_PAPER[policy]}}} & "
            rf"{_format_pm_spaced(o_mean, o_std, o_dec, o_std_dec)} & "
            rf"{_format_pm_spaced(w_mean, w_std, w_dec, w_std_dec)} \\")
    tab.append(r"    \bottomrule")
    tab.append(r"  \end{tabular}")
    tab.append(r"\end{table}")

    return _snapshot_from_lines(tab, source="run_summaries")


def main() -> None:
    here = Path(__file__).resolve().parent
    paper_data = (here / ".." / "paper_data").resolve()
    summary_root = paper_data / "summary"
    run_summaries_csv = paper_data / "run_summaries.csv"
    snapshot_out = paper_data / "results_snapshot" / "tables_snapshot.json"

    if not run_summaries_csv.exists():
        _write_run_summaries_from_json(summary_root, run_summaries_csv)

    rows = _load_run_summaries(run_summaries_csv)
    table_ids = {r.get("table_id") for r in rows if r.get("table_id")}

    snapshot: Dict[str, Dict[str, List[str]]] = {}

    if "cliff_safe" in table_ids:
        snapshot["tab_cliff_safe.tex"] = _build_tab_cliff_safe(rows)

    if "baseline_zoo" in table_ids:
        snapshot["tab_baseline_zoo.tex"] = _build_tab_baseline_zoo(rows)

    if "controls" in table_ids:
        snapshot["tab_controls.tex"] = _build_tab_controls(rows)

    if "pro6000" in table_ids:
        snapshot["tab_pro6000_k4_m8.tex"] = _build_tab_pro6000(rows)

    if "bg_liveness" in table_ids:
        snapshot["tab_bg_liveness.tex"] = _build_tab_bg_liveness(rows)

    curated_dir = paper_data / "tables"
    skip_curated = {
        "tab_per_class_throughput_tok_eq.tex",
        "tab_vip_absence.tex",
    }
    for tex_path in curated_dir.glob("tab_*.tex"):
        if tex_path.name in skip_curated:
            continue
        if tex_path.name in snapshot:
            continue
        snapshot[tex_path.name] = _snapshot_from_curated(tex_path)

    snapshot_out.parent.mkdir(parents=True, exist_ok=True)
    with snapshot_out.open("w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2, ensure_ascii=False)

    print(f"Wrote {snapshot_out}")


if __name__ == "__main__":
    main()
