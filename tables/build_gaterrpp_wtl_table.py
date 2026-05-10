from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

COMPARISONS = [
    {
        "label": "\\texttt{gate\\_rr\\_pp} vs.\\ \\texttt{gate\\_rr}",
        "left": "gate_rr_pp",
        "right": "gate_rr",
        "sources": [
            "server_pull_main_matrix_0114_pp*/runs",
            "server_pull_verify_gaterrpp_0114/runs",
            "server_pull_gaterrpp_check_0114/runs",
        ],
        "workloads": ["W1_main", "W2_phase"],
    },
    {
        "label": "\\texttt{gate\\_u} vs.\\ \\texttt{gate\\_rr} (hotcold)",
        "left": "gate_u",
        "right": "gate_rr",
        "sources": [
            "server_pull_gatemix_hotcold_0116_085912/gatemix_hotcold",
        ],
        "workloads": ["W1_hotcold"],
    },
]

K_TARGET = "4"
THR_TIE_EPS = 0.01  # rps


def _get(summary: dict, keys: list[str], default=None):
    for key in keys:
        if key in summary:
            return summary[key]
    return default


def _get_p99(summary: dict, base_keys: list[str], fallback_keys: list[str]):
    for key in fallback_keys:
        if key in summary:
            return summary[key]
    for key in base_keys:
        if key in summary and isinstance(summary[key], dict):
            return summary[key].get("p99")
    return None


def _arrival_total(entry: dict) -> float | None:
    vip = entry.get("arrival_vip")
    bg = entry.get("arrival_bg")
    if vip is None or bg is None:
        return None
    return float(vip) + float(bg)


def _ok_total(entry: dict) -> float | None:
    vip = entry.get("ok_vip")
    bg = entry.get("ok_bg")
    if vip is None or bg is None:
        return None
    return float(vip) + float(bg)


def _select_best(existing: dict, candidate: dict) -> dict:
    existing_arrival = _arrival_total(existing)
    candidate_arrival = _arrival_total(candidate)
    if candidate_arrival is not None and existing_arrival is not None:
        if candidate_arrival > existing_arrival:
            return candidate
        return existing
    if candidate_arrival is not None and existing_arrival is None:
        return candidate
    if candidate_arrival is None and existing_arrival is not None:
        return existing
    existing_ok = _ok_total(existing)
    candidate_ok = _ok_total(candidate)
    if candidate_ok is not None and existing_ok is not None and candidate_ok > existing_ok:
        return candidate
    return existing


def _mean(values: list[float]) -> float | None:
    values = [v for v in values if v is not None]
    if not values:
        return None
    return sum(values) / len(values)


def _wtl_for_workload(means: dict, workload: str, left: str, right: str):
    wins = {"vip": 0, "bg": 0, "thr": 0}
    ties = {"vip": 0, "bg": 0, "thr": 0}
    losses = {"vip": 0, "bg": 0, "thr": 0}
    metrics = {"vip": "low", "bg": "low", "thr": "high"}
    for (wl, _k, _rank), data in means.items():
        if wl != workload:
            continue
        if left not in data or right not in data:
            continue
        for metric, mode in metrics.items():
            a = data[left].get(metric)
            b = data[right].get(metric)
            if a is None or b is None:
                continue
            diff = a - b
            if metric == "thr":
                if abs(diff) <= THR_TIE_EPS:
                    ties[metric] += 1
                    continue
            if abs(diff) <= 1e-9:
                ties[metric] += 1
                continue
            if mode == "low":
                if diff < 0:
                    wins[metric] += 1
                else:
                    losses[metric] += 1
            else:
                if diff > 0:
                    wins[metric] += 1
                else:
                    losses[metric] += 1
    return wins, ties, losses


def _collect_entries(base: Path, sources: list[str], policies: set[str]) -> list[dict]:
    runs_dirs = []
    for pattern in sources:
        for d in base.glob(pattern):
            if d.is_dir():
                runs_dirs.append(d)

    entries = []
    for runs in runs_dirs:
        for summary_path in runs.glob("**/summary.json"):
            try:
                summary = json.loads(summary_path.read_text())
            except Exception:
                continue
            policy = _get(summary, ["policy", "policy_id"])
            if policy not in policies:
                continue
            entry = {
                "policy": policy,
                "workload": _get(summary, ["workload_id", "workload"]),
                "k": _get(summary, ["k", "vllm_max_loras", "max_loras"]),
                "rank": _get(summary, ["vllm_max_lora_rank", "max_lora_rank", "rank"]),
                "seed": _get(summary, ["seed"]),
                "vip_p99": _get_p99(
                    summary,
                    ["vip_ttft_ms"],
                    ["vip_ttft_ms.p99", "vip_ttft_p99_ms", "vip_ttft_p99"],
                ),
                "bg_p99": _get_p99(
                    summary,
                    ["bg_ttft_ms"],
                    ["bg_ttft_ms.p99", "bg_ttft_p99_ms", "bg_ttft_p99"],
                ),
                "thr": _get(summary, ["throughput_rps", "thr_rps"]),
                "arrival_vip": _get(summary, ["arrival_count_vip"]),
                "arrival_bg": _get(summary, ["arrival_count_bg"]),
                "ok_vip": _get(summary, ["ok_count_vip"]),
                "ok_bg": _get(summary, ["ok_count_bg"]),
                "path": str(summary_path),
            }
            entries.append(entry)
    return entries


def _dedupe_entries(entries: list[dict]) -> list[dict]:
    selected = {}
    for entry in entries:
        key = (
            entry["policy"],
            entry["workload"],
            str(entry["k"]),
            str(entry["rank"]),
            entry["seed"],
        )
        if key not in selected:
            selected[key] = entry
            continue
        selected[key] = _select_best(selected[key], entry)
    return list(selected.values())


def _build_means(entries: list[dict], workloads: list[str]) -> dict:
    entries = [
        e
        for e in entries
        if str(e["k"]) == K_TARGET and e["workload"] in workloads
    ]

    by_group = defaultdict(list)
    for entry in entries:
        by_group[
            (entry["workload"], str(entry["k"]), str(entry["rank"]), entry["policy"])
        ].append(entry)

    means = defaultdict(dict)
    for (workload, k, rank, policy), items in by_group.items():
        means[(workload, k, rank)][policy] = {
            "vip": _mean([float(i["vip_p99"]) for i in items if i["vip_p99"] is not None]),
            "bg": _mean([float(i["bg_p99"]) for i in items if i["bg_p99"] is not None]),
            "thr": _mean([float(i["thr"]) for i in items if i["thr"] is not None]),
        }
    return means


def _fmt(w: int, t: int, l: int) -> str:
    return f"{w}/{t}/{l}"


def _latex_tt(text: str) -> str:
    return "\\texttt{" + text.replace("_", "\\_") + "}"


def _total_counts(wins: dict, ties: dict, losses: dict) -> int:
    return sum(wins.values()) + sum(ties.values()) + sum(losses.values())


def main() -> None:
    base = Path(__file__).resolve().parents[1]
    row_lines = []
    for comp in COMPARISONS:
        policies = {comp["left"], comp["right"]}
        entries = _collect_entries(base, comp["sources"], policies)
        entries = _dedupe_entries(entries)
        means = _build_means(entries, comp["workloads"])

        overall_w = {"vip": 0, "bg": 0, "thr": 0}
        overall_t = {"vip": 0, "bg": 0, "thr": 0}
        overall_l = {"vip": 0, "bg": 0, "thr": 0}
        section_rows = []
        for workload in comp["workloads"]:
            wins, ties, losses = _wtl_for_workload(
                means, workload, comp["left"], comp["right"]
            )
            if _total_counts(wins, ties, losses) == 0:
                continue
            overall_w = {k: overall_w[k] + wins[k] for k in overall_w}
            overall_t = {k: overall_t[k] + ties[k] for k in overall_t}
            overall_l = {k: overall_l[k] + losses[k] for k in overall_l}
            section_rows.append(
                f"{comp['label']} & {_latex_tt(workload)} & "
                f"{_fmt(wins['vip'], ties['vip'], losses['vip'])} & "
                f"{_fmt(wins['bg'], ties['bg'], losses['bg'])} & "
                f"{_fmt(wins['thr'], ties['thr'], losses['thr'])} \\\\"
            )

        if _total_counts(overall_w, overall_t, overall_l) > 0:
            section_rows.append(
                f"{comp['label']} & Overall & "
                f"{_fmt(overall_w['vip'], overall_t['vip'], overall_l['vip'])} & "
                f"{_fmt(overall_w['bg'], overall_t['bg'], overall_l['bg'])} & "
                f"{_fmt(overall_w['thr'], overall_t['thr'], overall_l['thr'])} \\\\"
            )

        if not section_rows:
            continue
        if row_lines:
            row_lines.append("\\midrule")
        row_lines.extend(section_rows)

    rows = "\n".join(row_lines)
    table = f"""\\begin{{table}}[t]
\\centering
\\small
\\setlength{{\\tabcolsep}}{{6pt}}
\\begin{{tabular}}{{llccc}}
\\toprule
\\textbf{{Comparison}} & \\textbf{{Workload (K=4)}} & \\textbf{{VIP TTFT p99}} & \\textbf{{BG TTFT p99}} & \\textbf{{Throughput}} \\\\
\\midrule
{rows}
\\bottomrule
\\end{{tabular}}
\\vspace{{2pt}}
\\caption{{Win/tie/loss summary for K=4 explorations. The first block compares \\texttt{{gate\\_rr\\_pp}} vs.\\ \\texttt{{gate\\_rr}} over W1\\_main and W2\\_phase; the second compares \\texttt{{gate\\_u}} vs.\\ \\texttt{{gate\\_rr}} over W1\\_hotcold from gatemix\\_hotcold (3 seeds). Each entry is counted over grouped settings (workload\\_id, k, vllm\\_max\\_lora\\_rank) using the mean across seeds per group.}}
\\label{{tab:classdrr_wtl}}
\\end{{table}}
"""

    out_paths = [
        base / "docs" / "figures" / "tab_gaterrpp_wtl.tex",
        base / "figures" / "tab_gaterrpp_wtl.tex",
    ]
    for out_path in out_paths:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(table)
        print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
