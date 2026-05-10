#!/usr/bin/env python3
"""Plot Fig.3 phase mechanism alignment (GlobalFIFO vs CLIMB) from run logs.

Usage:
  python plot_phase_mech.py \
    --vanilla /path/to/globalfifo/run_dir \
    --climb /path/to/gate_rr/run_dir \
    --k 4

Outputs:
  figures/fig_phase_mech.pdf
  figures/fig_phase_mech.png
"""
from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
import shutil

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for name in candidates:
        if name in df.columns:
            return name
    return None


def _normalize_time(series: pd.Series) -> pd.Series:
    vals = series.astype(float)
    vmax = vals.max()
    # Heuristic: epoch ns/ms/s or relative seconds.
    if vmax > 1e14:
        vals = vals / 1e9
    elif vmax > 1e11:
        vals = vals / 1e3
    return vals


def _find_adapter_col(df: pd.DataFrame) -> str | None:
    candidates = [
        "adapter_id",
        "adapter",
        "lora",
        "lora_name",
        "model",
        "model_id",
    ]
    return _pick_col(df, candidates)


def _vip_mask(df: pd.DataFrame) -> pd.Series:
    # Try boolean VIP column first.
    vip_col = _pick_col(df, ["is_vip", "vip", "is_vip_req"])
    if vip_col is not None:
        return df[vip_col].astype(bool)

    # Try class/tier label.
    tier_col = _pick_col(df, ["tier", "class", "priority"])
    if tier_col is not None:
        return df[tier_col].astype(str).str.contains("vip", case=False, na=False)

    # Fall back to adapter/model name.
    adapter_col = _find_adapter_col(df)
    if adapter_col is not None:
        return df[adapter_col].astype(str).str.contains("vip", case=False, na=False)

    raise ValueError("Cannot infer VIP requests: no vip/tier/adapter columns found.")


def _rolling_p99(
    times_s: np.ndarray,
    values_s: np.ndarray,
    t_min: float,
    t_max: float,
    window_s: float = 10.0,
    step_s: float = 1.0,
    min_samples: int = 50,
) -> tuple[np.ndarray, np.ndarray]:
    t_grid = np.arange(t_min, t_max + step_s, step_s)
    out = np.full_like(t_grid, np.nan, dtype=float)
    for i, t in enumerate(t_grid):
        lo = t - window_s
        mask = (times_s >= lo) & (times_s < t)
        if np.count_nonzero(mask) >= min_samples:
            out[i] = np.nanpercentile(values_s[mask], 99)
    return t_grid, out


def _thrash_proxy_from_control(control: pd.DataFrame) -> tuple[np.ndarray, np.ndarray] | None:
    time_col = _pick_col(control, ["ts", "time", "t", "timestamp"])
    if time_col is None:
        return None

    t = _normalize_time(control[time_col])
    t = t - t.min()

    # Fallback: resident-set change rate from control_log.
    if "resident" in control.columns:
        res = control["resident"].astype(str).tolist()
        t_vals = t.to_numpy()
        changes = np.zeros(len(res), dtype=float)
        prev = None
        for i, s in enumerate(res):
            try:
                curr = set(ast.literal_eval(s))
            except Exception:
                curr = prev
            if prev is not None and curr != prev:
                changes[i] = 1.0
            prev = curr
        bins = np.arange(0, np.ceil(t_vals.max()) + 1)
        idx = np.clip(np.floor(t_vals).astype(int), 0, len(bins) - 1)
        per_sec = np.zeros(len(bins))
        for c, b in zip(changes, idx):
            per_sec[b] += c
        return bins, per_sec

    return None


def _thrash_proxy_from_requests(req: pd.DataFrame, time_col: str, adapter_col: str) -> tuple[np.ndarray, np.ndarray]:
    t = _normalize_time(req[time_col])
    t = t - t.min()
    adapter = req[adapter_col].astype(str).to_numpy()

    order = np.argsort(t.to_numpy())
    t_sorted = t.to_numpy()[order]
    adapter_sorted = adapter[order]
    switches = np.zeros_like(t_sorted, dtype=int)
    switches[1:] = (adapter_sorted[1:] != adapter_sorted[:-1]).astype(int)

    bins = np.arange(0, np.ceil(t_sorted.max()) + 1)
    idx = np.clip(np.floor(t_sorted).astype(int), 0, len(bins) - 1)
    per_sec = np.zeros(len(bins))
    for s, b in zip(switches, idx):
        per_sec[b] += s
    return bins, per_sec


def _active_set_from_control(control: pd.DataFrame) -> tuple[np.ndarray, np.ndarray] | None:
    candidates = [
        "active_set_size",
        "active_loras",
        "active_adapters",
        "admitted_loras",
        "active_count",
    ]
    col = _pick_col(control, candidates)
    time_col = _pick_col(control, ["ts", "time", "t", "timestamp"])
    if col is None or time_col is None:
        return None
    t = _normalize_time(control[time_col])
    t = t - t.min()
    return t.to_numpy(), control[col].astype(float).to_numpy()


def _load_requests(run_dir: Path) -> pd.DataFrame:
    path = run_dir / "requests_log.csv"
    if not path.exists():
        raise FileNotFoundError(f"requests_log.csv not found in {run_dir}")
    return pd.read_csv(path)


def _load_control(run_dir: Path) -> pd.DataFrame | None:
    path = run_dir / "control_log.csv"
    if not path.exists():
        return None
    return pd.read_csv(path)


def _compute_series(run_dir: Path) -> dict:
    req = _load_requests(run_dir)
    control = _load_control(run_dir)

    arr_col = _pick_col(req, ["arrival_ts", "arrival_time", "arrival", "t_arrival"])
    disp_col = _pick_col(req, ["dispatch_ts", "dispatch_time", "dispatch", "t_dispatch"])
    tok_col = _pick_col(req, ["first_token_ts", "first_token_time", "first_token", "t_first_token"])
    if arr_col is None or disp_col is None or tok_col is None:
        raise ValueError(
            f"Missing time columns in {run_dir}. "
            f"Columns: {list(req.columns)}"
        )

    arr = _normalize_time(req[arr_col])
    disp = _normalize_time(req[disp_col])
    tok = _normalize_time(req[tok_col])
    t0 = arr.min()
    arr = arr - t0
    disp = disp - t0
    tok = tok - t0

    vip_mask = _vip_mask(req)
    queue = (disp - arr).to_numpy()
    engine = (tok - disp).to_numpy()
    t_vip = arr[vip_mask].to_numpy()
    q_vip = queue[vip_mask.to_numpy()]
    e_vip = engine[vip_mask.to_numpy()]

    # Adapt min_samples to observed VIP rate to avoid empty panels at low RPS.
    duration = max(arr.max(), 1.0)
    vip_rate = len(t_vip) / duration
    window_s = 10.0
    min_samples = max(5, int(round(vip_rate * window_s * 0.3)))

    t_grid, q_p99 = _rolling_p99(t_vip, q_vip, 0.0, 600.0, window_s=window_s, min_samples=min_samples)
    _, e_p99 = _rolling_p99(t_vip, e_vip, 0.0, 600.0, window_s=window_s, min_samples=min_samples)

    if control is not None:
        active = _active_set_from_control(control)
        thrash = _thrash_proxy_from_control(control)
    else:
        active = None
        thrash = None

    return {
        "t_grid": t_grid,
        "q_p99": q_p99,
        "e_p99": e_p99,
        "active": active,
        "thrash": thrash,
    }


def _plot_panel(ax, t_grid, q_p99, e_p99, title: str) -> None:
    q = np.nan_to_num(q_p99, nan=0.0)
    e = np.nan_to_num(e_p99, nan=0.0)
    total = q + e
    ax.fill_between(t_grid, 0, q, color="#0072B2", alpha=0.25, label="queue p99")
    ax.fill_between(t_grid, q, total, color="#D55E00", alpha=0.25, label="engine p99")
    ax.plot(t_grid, q, color="#0072B2", linewidth=1.0)
    ax.plot(t_grid, total, color="#D55E00", linewidth=1.0)
    ax.set_ylim(0, 60)
    ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.5)
    ax.set_title(title)


def _load_paper_timeseries(paper_dir: Path) -> dict[str, dict] | None:
    data_path = paper_dir / "timeseries_phase_mech.csv"
    if not data_path.exists():
        return None
    df = pd.read_csv(data_path)
    required = {"t_s", "policy", "queue_p99_s", "engine_p99_s"}
    if not required.issubset(df.columns):
        raise ValueError(
            f"{data_path} missing columns: {sorted(required - set(df.columns))}"
        )
    out: dict[str, dict] = {}
    for policy in ["GlobalFIFO", "CLIMB"]:
        sub = df[df["policy"] == policy].sort_values("t_s")
        if sub.empty:
            raise ValueError(f"No rows for policy={policy} in {data_path}")
        t_grid = sub["t_s"].to_numpy(dtype=float)
        q = sub["queue_p99_s"].to_numpy(dtype=float)
        e = sub["engine_p99_s"].to_numpy(dtype=float)
        thrash = None
        if "thrash_per_s" in sub.columns:
            thrash = (t_grid, sub["thrash_per_s"].to_numpy(dtype=float))
        out[policy] = {
            "t_grid": t_grid,
            "q_p99": q,
            "e_p99": e,
            "thrash": thrash,
        }
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vanilla", type=Path)
    parser.add_argument("--climb", type=Path)
    parser.add_argument("--k", type=float, default=4)
    parser.add_argument("--paper-data", action="store_true", help="Use paper_data/figures outputs.")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_dir = root / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)
    paper_dir = root / "paper_data" / "figures"

    if args.paper_data or (args.vanilla is None and args.climb is None):
        if not paper_dir.exists():
            raise SystemExit("paper_data/figures not found. Cannot run in paper-data mode.")
        series = _load_paper_timeseries(paper_dir)
        if series is not None:
            van = series["GlobalFIFO"]
            cli = series["CLIMB"]
        else:
            snapshot = paper_dir / "fig_phase_mech.snapshot.json"
            if snapshot.exists():
                data = json.loads(snapshot.read_text())
                files = data.get("files", {})
                for key in ("pdf", "png"):
                    name = files.get(key)
                    if not name:
                        continue
                    src = paper_dir / name
                    if src.exists():
                        shutil.copy2(src, out_dir / Path(name).name)
                        print(f"Wrote figures/{Path(name).name}")
                return
            for name in ["fig_phase_mech.pdf", "fig_phase_mech.png"]:
                src = paper_dir / name
                if src.exists():
                    shutil.copy2(src, out_dir / name)
                    print(f"Wrote figures/{name}")
            return
    else:
        if args.vanilla is None or args.climb is None:
            raise SystemExit("Please provide --vanilla and --climb run dirs, or use --paper-data.")
        van = _compute_series(args.vanilla)
        cli = _compute_series(args.climb)

    fig, axes = plt.subplots(
        2, 2, figsize=(7.0, 2.9), sharex=True, constrained_layout=True
    )
    plt.rcParams.update({"font.size": 8})

    # Row 1: VIP rolling p99 (queue vs engine) stacked.
    for col, data, title in [(0, van, "GlobalFIFO"), (1, cli, "CLIMB")]:
        ax = axes[0, col]
        ax.axvline(200, color="0.6", linestyle="--", linewidth=0.8)
        ax.axvline(400, color="0.6", linestyle="--", linewidth=0.8)
        ax.axvspan(200, 400, color="0.9", alpha=0.6)
        _plot_panel(ax, data["t_grid"], data["q_p99"], data["e_p99"], title)
        if col == 0:
            ax.set_ylabel("VIP p99 (s)")
    axes[0, 0].legend(loc="upper right", frameon=False, fontsize=7)

    # Row 2: Thrash proxy (rolling mean of per-second counts).
    for col, data in [(0, van), (1, cli)]:
        ax = axes[1, col]
        ax.axvline(200, color="0.6", linestyle="--", linewidth=0.8)
        ax.axvline(400, color="0.6", linestyle="--", linewidth=0.8)
        ax.axvspan(200, 400, color="0.9", alpha=0.6)
        if data["thrash"] is not None:
            t, v = data["thrash"]
            window = 5
            kernel = np.ones(window) / window
            v_smooth = np.convolve(v, kernel, mode="same")
            ax.plot(t, v_smooth, color="0.3", linewidth=1.0)
        else:
            ax.plot([], [])
        ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.5)
        if col == 0:
            ax.set_ylabel("thrash proxy (/s)")
        ax.set_xlabel("Time (s)")

    for ax in axes.ravel():
        ax.set_xlim(150, 450)

    pdf_path = out_dir / "fig_phase_mech.pdf"
    png_path = out_dir / "fig_phase_mech.png"
    fig.savefig(pdf_path)
    fig.savefig(png_path, dpi=300)
    print("Wrote figures/fig_phase_mech.pdf")
    print("Wrote figures/fig_phase_mech.png")


if __name__ == "__main__":
    main()
