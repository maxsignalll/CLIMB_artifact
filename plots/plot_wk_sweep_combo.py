#!/usr/bin/env python3
"""Combine W-sweep and K-sweep into one side-by-side figure."""

from __future__ import annotations

from pathlib import Path
import json

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


ROOT = Path(__file__).resolve().parents[1]
PAPER_DATA_PATH = ROOT / "paper_data" / "figures" / "fig_wk_sweep_combo.json"

DEFAULT_W_SWEEP = {
    "w_vals": [5, 6, 7, 8],
    "vip_vanilla": [10.0, 41.5, 54.8, 41.9],
    "vip_vanilla_err": [1.3, 16.4, 19.2, 14.0],
    "vip_gate": [11.6, 9.8, 11.4, 13.8],
    "vip_gate_err": [0.2, 0.7, 4.2, 3.0],
    "thr_vanilla": [7.31, 8.41, 9.52, 10.65],
    "thr_vanilla_err": [0.16, 0.19, 0.18, 0.18],
    "thr_gate": [7.31, 8.42, 9.52, 10.66],
    "thr_gate_err": [0.17, 0.19, 0.18, 0.18],
}

DEFAULT_K_SWEEP = {
    "k_vals": [3, 4, 5, 6, 7, 8],
    "vip_vanilla": [33.70, 38.71, 31.97, 56.60, 42.05, 0.13],
    "vip_vanilla_err": [7.35, 13.18, 1.49, 9.44, 17.16, 0.00],
    "vip_gate": [9.80, 13.06, 24.54, 33.00, 63.45, 0.13],
    "vip_gate_err": [2.09, 3.19, 3.83, 5.83, 26.93, 0.00],
    "thr_vanilla": [10.65, 10.66, 10.66, 10.66, 10.66, 10.66],
    "thr_vanilla_err": [0.18, 0.18, 0.18, 0.18, 0.22, 0.18],
    "thr_gate": [10.65, 10.66, 10.66, 10.66, 10.66, 10.66],
    "thr_gate_err": [0.18, 0.18, 0.18, 0.18, 0.22, 0.18],
}


def _load_paper_data() -> dict | None:
    if not PAPER_DATA_PATH.exists():
        return None
    with PAPER_DATA_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _merge_defaults(defaults: dict, override: dict | None) -> dict:
    merged = dict(defaults)
    if override:
        for key, value in override.items():
            merged[key] = value
    return merged


def plot_w_sweep(ax_top, ax_bot, data: dict) -> None:
    w_vals = np.array(data["w_vals"], dtype=float)

    vip_vanilla = np.array(data["vip_vanilla"])
    vip_vanilla_err = np.array(data["vip_vanilla_err"])
    vip_gate = np.array(data["vip_gate"])
    vip_gate_err = np.array(data["vip_gate_err"])

    thr_vanilla = np.array(data["thr_vanilla"])
    thr_vanilla_err = np.array(data["thr_vanilla_err"])
    thr_gate = np.array(data["thr_gate"])
    thr_gate_err = np.array(data["thr_gate_err"])

    ax_top.errorbar(
        w_vals,
        vip_vanilla,
        yerr=vip_vanilla_err,
        label="GlobalFIFO",
        color="#0072B2",
        linestyle="-",
        marker="o",
        markersize=4,
        linewidth=1.2,
        capsize=2,
    )
    ax_top.errorbar(
        w_vals,
        vip_gate,
        yerr=vip_gate_err,
        label="CLIMB",
        color="#D55E00",
        linestyle="--",
        marker="s",
        markersize=4,
        linewidth=1.2,
        capsize=2,
    )
    ax_top.set_ylabel("VIP TTFT p99 (s)")
    ax_top.grid(True, which="both", linestyle=":", linewidth=0.5, alpha=0.6)
    ax_top.set_title("(a) W-sweep: VIP TTFT p99", loc="left", pad=2, fontsize=8)
    ax_top.annotate(
        "cliff onset",
        xy=(6.0, vip_vanilla[1]),
        xytext=(5.83, 65.5),
        textcoords="data",
        fontsize=7,
        arrowprops=dict(
            arrowstyle="->",
            color="0.25",
            linewidth=0.8,
            shrinkA=0,
            shrinkB=9,
        ),
    )
    ax_top.legend(
        loc="upper left",
        frameon=True,
        facecolor="white",
        framealpha=0.85,
    )

    ax_bot.errorbar(
        w_vals,
        thr_vanilla,
        yerr=thr_vanilla_err,
        color="#0072B2",
        linestyle="-",
        marker="o",
        markersize=4,
        linewidth=1.2,
        capsize=2,
    )
    ax_bot.errorbar(
        w_vals,
        thr_gate,
        yerr=thr_gate_err,
        color="#D55E00",
        linestyle="--",
        marker="s",
        markersize=4,
        linewidth=1.2,
        capsize=2,
    )
    ax_bot.set_ylabel("Throughput (rps)")
    ax_bot.set_xlabel("Working set size W (distinct adapters)", labelpad=2)
    ax_bot.grid(True, which="both", linestyle=":", linewidth=0.5, alpha=0.6)
    ax_bot.set_title("(b) W-sweep: Throughput", loc="left", pad=2, fontsize=8)
    ax_bot.text(6.0, 10.2, "≈ identical", fontsize=7, color="0.35")
    ax_bot.set_xticks([5, 6, 7, 8])
    ax_bot.set_xlim(3.8, 8.2)


def plot_k_sweep(ax_top, ax_bot, data: dict) -> None:
    k_vals = np.array(data["k_vals"], dtype=float)

    vip_vanilla = np.array(data["vip_vanilla"])
    vip_vanilla_err = np.array(data["vip_vanilla_err"])
    vip_gate = np.array(data["vip_gate"])
    vip_gate_err = np.array(data["vip_gate_err"])

    thr_vanilla = np.array(data["thr_vanilla"])
    thr_vanilla_err = np.array(data["thr_vanilla_err"])
    thr_gate = np.array(data["thr_gate"])
    thr_gate_err = np.array(data["thr_gate_err"])

    ax_top.errorbar(
        k_vals,
        vip_vanilla,
        yerr=vip_vanilla_err,
        label="GlobalFIFO",
        color="#0072B2",
        linestyle="-",
        marker="o",
        markersize=4,
        linewidth=1.2,
        capsize=2,
    )
    ax_top.errorbar(
        k_vals,
        vip_gate,
        yerr=vip_gate_err,
        label="CLIMB",
        color="#D55E00",
        linestyle="--",
        marker="s",
        markersize=4,
        linewidth=1.2,
        capsize=2,
    )
    ax_top.set_ylabel("VIP TTFT p99 (s)")
    ax_top.grid(True, which="both", linestyle=":", linewidth=0.5, alpha=0.6)
    ax_top.set_title("(c) K-sweep: VIP TTFT p99", loc="left", pad=2, fontsize=8)
    ax_top.axvline(8, linestyle="--", color="0.5", linewidth=0.8)
    ymin, _ = ax_top.get_ylim()
    ax_top.text(
        8.03,
        90.0,
        "K=W (fit boundary)",
        fontsize=6,
        rotation=90,
        va="top",
        color="0.4",
    )
    ax_top.set_ylim(ymin, 90.0)

    ax_bot.errorbar(
        k_vals,
        thr_vanilla,
        yerr=thr_vanilla_err,
        color="#0072B2",
        linestyle="-",
        marker="o",
        markersize=4,
        linewidth=1.2,
        capsize=2,
    )
    ax_bot.errorbar(
        k_vals,
        thr_gate,
        yerr=thr_gate_err,
        color="#D55E00",
        linestyle="--",
        marker="s",
        markersize=4,
        linewidth=1.2,
        capsize=2,
    )
    ax_bot.set_ylabel("Throughput (rps)")
    ax_bot.set_xlabel("Capacity K (max_loras)", labelpad=2)
    ax_bot.grid(True, which="both", linestyle=":", linewidth=0.5, alpha=0.6)
    ax_bot.set_title("(d) K-sweep: Throughput", loc="left", pad=2, fontsize=8)
    ax_bot.text(5.2, 10.9, "≈ identical", fontsize=7, color="0.35")
    ax_bot.set_ylim(10.0, 11.2)
    ax_bot.set_xticks([3, 4, 5, 6, 7, 8])
    ax_bot.set_xlim(2.7, 8.3)


def main() -> None:
    paper_data = _load_paper_data()
    w_data = _merge_defaults(DEFAULT_W_SWEEP, paper_data.get("w_sweep") if paper_data else None)
    k_data = _merge_defaults(DEFAULT_K_SWEEP, paper_data.get("k_sweep") if paper_data else None)
    plt.rcParams.update(
        {
            "font.size": 8,
            "axes.labelsize": 8,
            "legend.fontsize": 7,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
        }
    )
    fig, axes = plt.subplots(2, 2, figsize=(6.2, 3.02), sharex="col")
    plot_w_sweep(axes[0, 0], axes[1, 0], w_data)
    plot_k_sweep(axes[0, 1], axes[1, 1], k_data)

    # Only keep legend on the left panel for cleanliness.
    axes[0, 1].legend_.remove() if axes[0, 1].legend_ else None

    fig.subplots_adjust(left=0.08, right=0.99, top=0.96, bottom=0.20, wspace=0.35, hspace=0.35)

    out_dir = ROOT / "figures"
    out_dir.mkdir(parents=True, exist_ok=True)
    pdf_path = out_dir / "fig_wk_sweep_combo.pdf"
    png_path = out_dir / "fig_wk_sweep_combo.png"
    fig.savefig(pdf_path, bbox_inches="tight", pad_inches=0.02)
    fig.savefig(png_path, dpi=300, bbox_inches="tight", pad_inches=0.02)
    print("Wrote figures/fig_wk_sweep_combo.pdf")
    print("Wrote figures/fig_wk_sweep_combo.png")


if __name__ == "__main__":
    main()
