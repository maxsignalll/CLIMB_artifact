#!/usr/bin/env python3
"""Rank sweep heatmap for W2_phase (gate_rr), log2 fold-change vs pre/post baseline."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
import shutil
from typing import List, Optional, Tuple

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import TwoSlopeNorm
import numpy as np


RANKS = [8, 16, 32, 64, 128]
BASE_TPL = (
    "./server_pull_rank_sweep_2/"
    "runs/rank_sweep_2_r{r}/W2_phase/gate_rr"
)
WINDOW_S = 20.0
STEP_S = 2.0
T_MAX = 600.0
CLIFF_START = 200.0
CLIFF_END = 400.0
EPS = 1e-6


def list_seed_dirs(base: Path) -> List[Path]:
    return sorted(base.glob("seed=*"))


def load_vip_points(csv_path: Path) -> Tuple[List[float], List[float]]:
    times: List[float] = []
    vals: List[float] = []
    with csv_path.open("r", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ok = str(row.get("ok", "")).strip().lower()
            if ok not in {"true", "1", "yes"}:
                continue
            if row.get("class") != "VIP":
                continue
            arrival = row.get("arrival_ts")
            if not arrival:
                continue
            try:
                arr = float(arrival)
            except ValueError:
                continue
            ttft = row.get("ttft_ms")
            if ttft is None or ttft == "":
                ft = row.get("first_token_ts")
                if not ft:
                    continue
                try:
                    ttft = (float(ft) - arr) * 1000.0
                except ValueError:
                    continue
            try:
                ttft_s = float(ttft) / 1000.0
            except ValueError:
                continue
            times.append(arr)
            vals.append(ttft_s)
    if not times:
        return [], []
    t0 = min(times)
    times = [t - t0 for t in times]
    return times, vals


def rolling_p99(times: List[float], vals: List[float], t_grid: np.ndarray) -> np.ndarray:
    out = np.full(len(t_grid), np.nan, dtype=float)
    if not times:
        return out
    duration = max(times)
    vip_rate = len(times) / max(duration, 1.0)
    min_samples = max(5, int(round(vip_rate * WINDOW_S * 0.3)))
    for i, t in enumerate(t_grid):
        lo = t - WINDOW_S
        bucket = [v for tt, v in zip(times, vals) if lo <= tt < t]
        if len(bucket) >= min_samples:
            out[i] = np.percentile(bucket, 99)
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--paper-data", action="store_true", help="Use paper_data/figures outputs.")
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    out_dir = root / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)
    paper_dir = root / "paper_data" / "figures"

    if args.paper_data:
        if not paper_dir.exists():
            raise SystemExit("paper_data/figures not found. Cannot run in paper-data mode.")
        data_npz = paper_dir / "rank_sweep_heatmap.npz"
        if data_npz.exists():
            payload = np.load(data_npz)
            t_grid = payload["t_grid"]
            ranks = payload["ranks"]
            log2_fold = payload["log2_fold"]
            amplifications = payload["amplifications"]
            cliff_start = float(payload.get("cliff_start", CLIFF_START))
            cliff_end = float(payload.get("cliff_end", CLIFF_END))

            masked = np.ma.masked_invalid(log2_fold)
            vmin = np.nanpercentile(log2_fold, 5) if np.isfinite(log2_fold).any() else -1.0
            vmax = np.nanpercentile(log2_fold, 95) if np.isfinite(log2_fold).any() else 2.0
            vmin = min(vmin, -0.5)
            vmax = max(vmax, 1.5)

            plt.rcParams.update({"font.size": 8})
            fig, ax = plt.subplots(figsize=(5.2, 2.4))
            norm = TwoSlopeNorm(vcenter=0.0, vmin=vmin, vmax=vmax)
            im = ax.imshow(
                masked,
                aspect="auto",
                origin="lower",
                extent=[t_grid[0], t_grid[-1], 0, len(ranks)],
                cmap="coolwarm",
                norm=norm,
            )

            ax.set_yticks([i + 0.5 for i in range(len(ranks))])
            ax.set_yticklabels([str(int(r)) for r in ranks])
            ax.set_xlabel("Time (s)", labelpad=2)
            ax.set_ylabel("Rank")
            ax.axvline(cliff_start, color="0.3", linestyle="--", linewidth=0.8)
            ax.axvline(cliff_end, color="0.3", linestyle="--", linewidth=0.8)
            ax.set_xlim(t_grid[0], t_grid[-1])

            cbar = fig.colorbar(im, ax=ax, pad=0.01)
            cbar.set_label("log2 fold-change (VIP TTFT p99)")

            x_label = t_grid[-1] - 6.0
            for i, amp in enumerate(amplifications):
                if np.isfinite(amp):
                    label = f"A_r={amp:.1f}×"
                else:
                    label = "A_r=NA"
                y = i + 0.5
                ax.text(
                    x_label,
                    y,
                    label,
                    va="center",
                    ha="right",
                    fontsize=7,
                    bbox=dict(facecolor="white", edgecolor="none", alpha=0.6, pad=0.6),
                )
            fig.subplots_adjust(right=0.96, left=0.08, bottom=0.16, top=0.96)

            pdf_path = out_dir / "fig_rank_sweep_heatmap_w2.pdf"
            png_path = out_dir / "fig_rank_sweep_heatmap_w2.png"
            fig.savefig(pdf_path, bbox_inches="tight", pad_inches=0.02)
            fig.savefig(png_path, dpi=300, bbox_inches="tight", pad_inches=0.02)
            print("Wrote figures/fig_rank_sweep_heatmap_w2.pdf")
            print("Wrote figures/fig_rank_sweep_heatmap_w2.png")
            return

        snapshot = paper_dir / "fig_rank_sweep_heatmap_w2.snapshot.json"
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
        else:
            for name in ["fig_rank_sweep_heatmap_w2.pdf", "fig_rank_sweep_heatmap_w2.png"]:
                src = paper_dir / name
                if src.exists():
                    shutil.copy2(src, out_dir / name)
                    print(f"Wrote figures/{name}")
        return

    t_grid = np.arange(0.0, T_MAX + STEP_S, STEP_S)
    rows = []
    amplifications = []

    for r in RANKS:
        base = Path(BASE_TPL.format(r=r))
        seed_dirs = list_seed_dirs(base)
        if not seed_dirs:
            rows.append(np.full(len(t_grid), np.nan))
            amplifications.append(np.nan)
            continue

        per_seed = []
        for seed_dir in seed_dirs:
            csv_path = seed_dir / "requests_log.csv"
            if not csv_path.exists():
                continue
            times, vals = load_vip_points(csv_path)
            p99 = rolling_p99(times, vals, t_grid)
            baseline_mask = (t_grid < CLIFF_START) | (t_grid >= CLIFF_END)
            baseline_vals = p99[baseline_mask]
            baseline = np.nanmedian(baseline_vals) if np.isfinite(baseline_vals).any() else np.nan
            if not np.isfinite(baseline) or baseline <= 0:
                continue
            fold = p99 / max(baseline, EPS)
            per_seed.append(fold)

        if not per_seed:
            rows.append(np.full(len(t_grid), np.nan))
            amplifications.append(np.nan)
            continue

        fold_median = np.nanmedian(np.vstack(per_seed), axis=0)
        log2_fold = np.log2(np.maximum(fold_median, EPS))
        rows.append(log2_fold)
        cliff_mask = (t_grid >= CLIFF_START) & (t_grid < CLIFF_END)
        cliff_vals = fold_median[cliff_mask]
        amplifications.append(
            np.nanmedian(cliff_vals) if np.isfinite(cliff_vals).any() else np.nan
        )

    data = np.vstack(rows)
    masked = np.ma.masked_invalid(data)
    vmin = np.nanpercentile(data, 5) if np.isfinite(data).any() else -1.0
    vmax = np.nanpercentile(data, 95) if np.isfinite(data).any() else 2.0
    vmin = min(vmin, -0.5)
    vmax = max(vmax, 1.5)

    plt.rcParams.update({"font.size": 8})
    fig, ax = plt.subplots(figsize=(5.2, 2.4))
    norm = TwoSlopeNorm(vcenter=0.0, vmin=vmin, vmax=vmax)
    im = ax.imshow(
        masked,
        aspect="auto",
        origin="lower",
        extent=[t_grid[0], t_grid[-1], 0, len(RANKS)],
        cmap="coolwarm",
        norm=norm,
    )

    ax.set_yticks([i + 0.5 for i in range(len(RANKS))])
    ax.set_yticklabels([str(r) for r in RANKS])
    ax.set_xlabel("Time (s)", labelpad=2)
    ax.set_ylabel("Rank")
    ax.axvline(CLIFF_START, color="0.3", linestyle="--", linewidth=0.8)
    ax.axvline(CLIFF_END, color="0.3", linestyle="--", linewidth=0.8)
    ax.set_xlim(0, T_MAX)

    cbar = fig.colorbar(im, ax=ax, pad=0.01)
    cbar.set_label("log2 fold-change (VIP TTFT p99)")

    x_label = T_MAX - 6.0
    for i, amp in enumerate(amplifications):
        if np.isfinite(amp):
            label = f"A_r={amp:.1f}×"
        else:
            label = "A_r=NA"
        y = i + 0.5
        ax.text(
            x_label,
            y,
            label,
            va="center",
            ha="right",
            fontsize=7,
            bbox=dict(facecolor="white", edgecolor="none", alpha=0.6, pad=0.6),
        )
    fig.subplots_adjust(right=0.96, left=0.08, bottom=0.16, top=0.96)

    pdf_path = out_dir / "fig_rank_sweep_heatmap_w2.pdf"
    png_path = out_dir / "fig_rank_sweep_heatmap_w2.png"
    fig.savefig(pdf_path, bbox_inches="tight", pad_inches=0.02)
    fig.savefig(png_path, dpi=300, bbox_inches="tight", pad_inches=0.02)
    print("Wrote figures/fig_rank_sweep_heatmap_w2.pdf")
    print("Wrote figures/fig_rank_sweep_heatmap_w2.png")


if __name__ == "__main__":
    main()
