#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path


def write_lines(out_path: Path, lines: list[str]) -> None:
    out_path.write_text("\n".join(lines) + "\n")


def write_table(out_path: Path, payload: dict) -> None:
    lines = []
    lines.extend(payload.get("pre_lines", []))
    lines.append(payload["tabular_begin"])
    lines.extend(payload.get("tabular_lines", []))
    lines.append(payload["tabular_end"])
    lines.extend(payload.get("post_lines", []))
    out_path.write_text("\n".join(lines) + "\n")


def render_curated_min(outdir: Path) -> None:
    curated_path = Path("paper_data/results_snapshot/curated_min.json")
    if not curated_path.exists():
        return
    with curated_path.open("r", encoding="utf-8") as f:
        curated = json.load(f)

    # tab_overhead_appendix
    if "tab_overhead_appendix" in curated:
        rows = curated["tab_overhead_appendix"]["rows"]
        lines = [
            r"\begin{table}[t]",
            r"  \centering",
            r"  \small",
            r"  \caption{\textbf{Controller overhead (Appendix).} Detailed overhead statistics. Note that \texttt{GlobalFIFO} has no controller overhead.}",
            r"  \label{tab:overhead_ext}",
            r"  \setlength{\tabcolsep}{6pt} % 稍微增加列间距，因为现在空间很富裕",
            r"  % 核心修改：第5列由 p{4cm} 改为 l",
            r"  \begin{tabular}{l cc c l c c}",
            r"    \toprule",
            r"    & \multicolumn{2}{c}{\textbf{Ctrl Time ($\mu$s)}} & \textbf{State} & \textbf{Scaling over $K$ ($\mu$s)} & \textbf{Switch Rate (/s)} & \textbf{L/E (/s)} \\",
            r"    % 调整 cmidrule，使其只覆盖由多列合并的表头或特定组，单独一列通常不需要上方划线，除非为了分组",
            r"    \cmidrule(lr){2-3} \cmidrule(lr){5-5} \cmidrule(lr){6-6} ",
            r"    \textbf{Policy} & Mean & P99 & (KB) & & Mean$\pm$Std / P99 & Mean \\",
            r"    \midrule",
        ]
        for row in rows:
            policy = row["policy"]
            if policy == "GlobalFIFO":
                lines.append(
                    rf"    \textsc{{{policy}}} & {row['mean']} & {row['p99']} & {row['state']} & \multicolumn{{1}}{{c}}{{{row['scaling']}}} & {row['switch']} & {row['le']} \\"
                )
            else:
                lines.append(
                    rf"    \textsc{{{policy}}} & {row['mean']} & {row['p99']} & {row['state']} & "
                )
                lines.append(
                    rf"    {row['scaling']} & "
                )
                lines.append(
                    rf"    {row['switch']} & {row['le']} \\"
                )
        lines.extend([r"    \bottomrule", r"  \end{tabular}", r"\end{table}"])
        write_lines(outdir / "tab_overhead_appendix.tex", lines)
        print(f"Wrote {os.path.relpath(outdir / 'tab_overhead_appendix.tex', Path.cwd())}")

    # tab_variant_mechanisms
    if "tab_variant_mechanisms" in curated:
        rows = curated["tab_variant_mechanisms"]["rows"]
        lines = [
            r"\begin{table}[t]",
            r"\centering",
            r"\small",
            r"% 增加行高，防止文字过于密集",
            r"\renewcommand{\arraystretch}{1.2}",
            r"% @{} 去掉表格左右两端的默认空白，让表格完全对齐版心",
            r"\caption{Policy variants considered in design-space exploration. \textbf{Tag} denotes the short label used in plot legends.}",
            r"\begin{tabularx}{\textwidth}{@{} l l >{\raggedright\arraybackslash}X @{}}",
            r"\toprule",
            r"\textbf{Policy (paper name)} & \textbf{Tag} & \textbf{Definition} \\",
            r"\midrule",
        ]
        for idx, row in enumerate(rows):
            lines.append(rf"\textsc{{{row['policy']}}} & \textsc{{{row['tag']}}} &")
            parts = row["definition"].split("\\n")
            if len(parts) == 1:
                lines.append(rf"{parts[0]} \\")
            else:
                lines.append(rf"{parts[0]}")
                lines.append(rf"{parts[1]} \\")
            if idx != len(rows) - 1:
                lines.append("")
        lines.extend([r"\bottomrule", r"\end{tabularx}", "", r"\label{tab:policy_variants}", r"\end{table}"])
        write_lines(outdir / "tab_variant_mechanisms.tex", lines)
        print(f"Wrote {os.path.relpath(outdir / 'tab_variant_mechanisms.tex', Path.cwd())}")

    # tab_gaterrpp_wtl
    if "tab_gaterrpp_wtl" in curated:
        rows = curated["tab_gaterrpp_wtl"]["rows"]
        lines = [
            r"\begin{table}[t]",
            r"\centering",
            r"\small",
            r"\setlength{\tabcolsep}{6pt}",
            r"\caption{Win/tie/loss summary for \textsc{ClassDRR} vs.\ \textsc{CLIMB} in design-space exploration.",
            r"Each entry is counted over configuration groups (e.g., $(W,\lambda_{\text{VIP}},\lambda_{\text{BG}},\texttt{vllm\_max\_lora\_rank})$),",
            r"using the mean across seeds per group.}",
            r"\begin{tabular}{lccc}",
            r"\toprule",
            r"\textbf{Workload (K=4)} & \textbf{\textsf{VIP} TTFT p99} & \textbf{\textsf{BG} TTFT p99} & \textbf{Throughput} \\",
            r"\midrule",
        ]
        main_rows = [r for r in rows if r["workload"] != "Overall"]
        overall = next((r for r in rows if r["workload"] == "Overall"), None)
        for row in main_rows:
            pad = row.get("pad", " ")
            lines.append(rf"\texttt{{{row['workload']}}}{pad}& {row['vip']} & {row['bg']} & {row['thr']} \\")
        lines.append(r"\midrule")
        if overall is not None:
            pad = overall.get("pad", " ")
            lines.append(rf"{overall['workload']}{pad}& {overall['vip']} & {overall['bg']} & {overall['thr']} \\")
        lines.extend([r"\bottomrule", r"\end{tabular}", r"\vspace{2pt}", "", r"\label{tab:classdrr_wtl}", r"\end{table}"])
        write_lines(outdir / "tab_gaterrpp_wtl.tex", lines)
        print(f"Wrote {os.path.relpath(outdir / 'tab_gaterrpp_wtl.tex', Path.cwd())}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Render LaTeX tables from snapshot JSON.")
    default_snapshot = Path("paper_data/results_snapshot/tables_snapshot.json")
    default_outdir = Path("results/summary")
    parser.add_argument("--snapshot", type=Path, default=default_snapshot)
    parser.add_argument("--outdir", type=Path, default=default_outdir)
    args = parser.parse_args()

    snapshot_path = args.snapshot
    outdir = args.outdir
    outdir.mkdir(parents=True, exist_ok=True)

    with snapshot_path.open("r", encoding="utf-8") as f:
        snapshot = json.load(f)

    for filename, payload in snapshot.items():
        out_path = outdir / filename
        write_table(out_path, payload)
        rel = os.path.relpath(out_path, Path.cwd())
        print(f"Wrote {rel}")

    # Render curated-min tables (overrides snapshot versions if present)
    render_curated_min(outdir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
