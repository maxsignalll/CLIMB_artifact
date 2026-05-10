#!/usr/bin/env python3
"""Build a short Markdown summary from report CSVs produced by run_wk_sweep.sh.

Expected inputs:
  - report/hol_Ksweep_M8_k4_mp.csv
  - report/hol_Ksweep_M8_k8_mp.csv

Outputs:
  - reports/gpu_run_summary.md
"""

from __future__ import annotations

import argparse
from datetime import datetime
import re
from pathlib import Path
import pandas as pd
import json


def load_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)
    # Try to recover policy/workload from run_path when columns are NaN.
    if "run_path" in df.columns:
        def _parse(path_str: str) -> dict:
            parts = Path(path_str).parts
            out = {"workload_id": None, "policy_id": None}
            try:
                i = parts.index("runs")
                # runs/<exp>/<workload>/<policy>/<run_id>/...
                out["workload_id"] = parts[i + 2] if len(parts) > i + 2 else None
                out["policy_id"] = parts[i + 3] if len(parts) > i + 3 else None
            except ValueError:
                pass
            return out

        if "workload_id" not in df.columns:
            df["workload_id"] = None
        if "policy_id" not in df.columns:
            df["policy_id"] = None
        for idx, row in df.iterrows():
            if pd.isna(row.get("workload_id")) or pd.isna(row.get("policy_id")):
                meta = _parse(str(row.get("run_path", "")))
                df.at[idx, "workload_id"] = meta.get("workload_id")
                df.at[idx, "policy_id"] = meta.get("policy_id")
    return df


def pick_cols(df: pd.DataFrame):
    # Handle possible column names
    def col(*candidates):
        for c in candidates:
            if c in df.columns:
                return c
        return None

    return {
        "policy": col("policy_id", "policy"),
        "k": col("k"),
        "vip_ttft_p99": col("vip_ttft_ms_p99", "vip_ttft_p99"),
        "vip_queue_p99": col("vip_queue_ms_p99", "vip_queue_p99"),
        "vip_engine_p99": col("vip_engine_ms_p99", "vip_engine_p99"),
        "throughput": col("throughput_rps"),
        "run_path": col("run_path"),
        "workload": col("workload_id"),
    }


def fmt(x, nd=2):
    if x is None:
        return "NA"
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return "NA"


def extract_ts(row: pd.Series) -> datetime | None:
    """Extract timestamp from run_path/run_id like ts=YYYYMMDD-HHMMSS."""
    for key in ("run_path", "run_id"):
        val = row.get(key)
        if not isinstance(val, str):
            continue
        m = re.search(r"ts=(\d{8}-\d{6})", val)
        if m:
            try:
                return datetime.strptime(m.group(1), "%Y%m%d-%H%M%S")
            except Exception:
                return None
    return None


def read_summary_engine_p99(run_dir: Path) -> float | None:
    summary_path = run_dir / "summary.json"
    if not summary_path.exists():
        return None
    try:
        obj = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    vip_engine = obj.get("vip_engine_ms", {})
    if isinstance(vip_engine, dict):
        val = vip_engine.get("p99")
        if val is not None:
            return float(val)
    return None


def read_meta_start_ts(run_dir: Path) -> float | None:
    meta_path = run_dir / "meta.json"
    if not meta_path.exists():
        return None
    try:
        obj = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    start = obj.get("start_ts")
    return float(start) if start is not None else None


def read_summary_warmup_s(run_dir: Path) -> float | None:
    summary_path = run_dir / "summary.json"
    if not summary_path.exists():
        return None
    try:
        obj = json.loads(summary_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    warmup = obj.get("warmup_s")
    return float(warmup) if warmup is not None else None


def read_requests_engine_p99(run_dir: Path, start_ts: float | None = None, warmup_s: float | None = None) -> float | None:
    req_path = run_dir / "requests_log.csv"
    if not req_path.exists():
        return None
    usecols = ["class", "ok", "arrival_ts", "engine_ms"]
    try:
        df = pd.read_csv(req_path, usecols=usecols)
    except Exception:
        return None
    df = df[(df["class"] == "VIP") & (df["ok"] == True)]
    if start_ts is not None and warmup_s is not None:
        cutoff = start_ts + max(0.0, warmup_s)
        df = df[df["arrival_ts"] >= cutoff]
    vals = df["engine_ms"].dropna()
    if vals.empty:
        return None
    return float(vals.quantile(0.99))


def summarize_latest(df: pd.DataFrame, repo_root: Path) -> pd.DataFrame:
    cols = pick_cols(df)
    policy_col = cols["policy"] or "policy_id"
    if policy_col not in df.columns:
        df[policy_col] = "NA"
    metrics = [c for c in [cols["vip_ttft_p99"], cols["vip_queue_p99"], cols["vip_engine_p99"], cols["throughput"]] if c]
    if metrics:
        df = df.dropna(subset=metrics, how="all")
    if df.empty:
        return df

    df = df.copy()
    df["_ts"] = df.apply(extract_ts, axis=1)

    grouped = df.groupby(policy_col, dropna=False, sort=True)
    rows = []
    for policy, g in grouped:
        # Prefer latest timestamp; fallback to last row if timestamps missing.
        g_ts = g.dropna(subset=["_ts"])
        if not g_ts.empty:
            row = g_ts.sort_values("_ts").iloc[-1]
        else:
            row = g.iloc[-1]
        run_path = row.get("run_path")
        run_dir = None
        if isinstance(run_path, str):
            run_dir = (repo_root / run_path).resolve()
        engine_p99 = None
        if run_dir is not None:
            engine_p99 = read_summary_engine_p99(run_dir)
            if engine_p99 is None:
                start_ts = read_meta_start_ts(run_dir)
                warmup_s = read_summary_warmup_s(run_dir)
                engine_p99 = read_requests_engine_p99(run_dir, start_ts=start_ts, warmup_s=warmup_s)
        rows.append(
            {
                "policy": policy,
                "vip_ttft_p99": row.get(cols["vip_ttft_p99"], None),
                "vip_queue_p99": row.get(cols["vip_queue_p99"], None),
                "vip_engine_p99": engine_p99 if engine_p99 is not None else row.get(cols["vip_engine_p99"], None),
                "throughput": row.get(cols["throughput"], None),
            }
        )
    return pd.DataFrame(rows)


def emit_table(rows: list[tuple[int, pd.DataFrame | None]]) -> str:
    lines = []
    lines.append("| K | Policy | VIP TTFT p99 (ms) | VIP queue p99 (ms) | VIP engine p99 (ms) | Throughput (rps) |")
    lines.append("|---:|---|---:|---:|---:|---:|")
    for k, df in rows:
        if df is None or df.empty:
            lines.append(f"| {k} | MISSING | NA | NA | NA | NA |")
            continue
        for _, row in df.iterrows():
            lines.append(
                "| {k} | {policy} | {ttft} | {queue} | {engine} | {thr} |".format(
                    k=k,
                    policy=row.get("policy", "NA"),
                    ttft=fmt(row.get("vip_ttft_p99")),
                    queue=fmt(row.get("vip_queue_p99")),
                    engine=fmt(row.get("vip_engine_p99")),
                    thr=fmt(row.get("throughput"), nd=3),
                )
            )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report-dir", default="report")
    ap.add_argument("--out", default="reports/gpu_run_summary.md")
    args = ap.parse_args()

    base_root = Path(__file__).resolve().parents[1]  # artifact repository root
    report_dir = base_root / args.report_dir
    repo_root = base_root
    if not report_dir.exists():
        fallback = base_root.parent / args.report_dir
        if fallback.exists():
            report_dir = fallback
            repo_root = base_root.parent
    k4_path = report_dir / "hol_Ksweep_M8_k4_mp.csv"
    k8_path = report_dir / "hol_Ksweep_M8_k8_mp.csv"

    rows: list[tuple[int, pd.DataFrame | None]] = []
    missing: list[Path] = []
    for k, p in [(4, k4_path), (8, k8_path)]:
        if p.exists():
            df = load_csv(p)
            rows.append((k, summarize_latest(df, repo_root)))
        else:
            rows.append((k, None))
            missing.append(p)

    workload = None
    # Workload is recorded in raw CSV; fallback to default if missing.
    for k, p in [(4, k4_path), (8, k8_path)]:
        if p.exists():
            df_raw = load_csv(p)
            if "workload_id" in df_raw.columns:
                workload = df_raw.get("workload_id", pd.Series([None])).iloc[0]
                if isinstance(workload, str) and workload:
                    break
    workload = workload if isinstance(workload, str) and workload else "W2_phase_hol_rps3_p2048_split_M8"

    out_path = base_root / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)

    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    lines = []
    lines.append("# GPU Run Summary (W2/M8, K=4/8)")
    lines.append("")
    lines.append("## Run settings")
    lines.append(f"- workload: `{workload}`")
    lines.append("- policies: `vanilla`, `gate_rr`")
    lines.append(f"- generated: {now}")
    lines.append("")
    lines.append("## Key metrics")
    lines.append("")
    lines.append(emit_table(rows))
    lines.append("")
    lines.append("## Artifacts")
    for k, p in [(4, k4_path), (8, k8_path)]:
        status = "found" if p.exists() else "missing"
        rel = p.relative_to(repo_root)
        lines.append(f"- {rel} ({status})")
    lines.append("")
    lines.append("## Notes")
    lines.append("- Values are read from the *_mp.csv reports produced by scripts/autodl/run_wk_sweep.sh.")
    lines.append("- For each policy, we select the latest run (by ts=YYYYMMDD-HHMMSS in run_path/run_id).")
    lines.append("- VIP engine p99 is taken from run summary.json when available; otherwise from requests_log.csv (warmup filtered when possible).")
    lines.append("- Rows with all key metrics missing are dropped before selection.")
    if missing:
        miss_rel = ", ".join(str(p.relative_to(repo_root)) for p in missing)
        lines.append(f"- Missing reports: {miss_rel}. Table rows are marked MISSING.")

    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    rel_out = out_path.relative_to(base_root)
    print(f"Wrote {rel_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
