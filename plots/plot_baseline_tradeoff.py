#!/usr/bin/env python3
"""Plot Fig 5.4: baseline trade-off scatter (tail vs throughput)."""

from itertools import cycle
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def size_from_thr(thr, thr_max=10.66, s_min=30.0, s_span=170.0):
    return s_min + (thr / thr_max) * s_span


PANELS = {
    "K=4 (cliff)": {
        "cache_aware": (40.96, 31.21, 10.66),
        "cap_only": (3.77, 118.40, 6.89),
        "gate_rr": (13.69, 40.36, 10.66),
        "no_switch": (0.05, 0.06, 3.99),
        "vanilla": (38.28, 19.76, 10.66),
    },
    "K=8 (safe)": {
        "cache_aware": (0.13, 0.15, 10.66),
        "cap_only": (3.85, 118.30, 6.78),
        "gate_rr": (0.13, 0.14, 10.66),
        "no_switch": (0.05, 0.06, 3.99),
        "vanilla": (0.13, 0.14, 10.66),
    },
}

POLICY_ORDER = ["cache_aware", "cap_only", "gate_rr", "no_switch", "vanilla"]
POLICY_LABEL = {
    "cache_aware": "LRU",
    "cap_only": "BGCap",
    "gate_rr": "CLIMB",
    "no_switch": "Lock",
    "vanilla": "FIFO",
}
CONFUNDED = {"no_switch"}
LOCK_DELTA = (-4, 4)
FIFO_LEFT_OFFSET = (-23.5, -1.5)
LABEL_OVERRIDES = {
    ("K=4 (cliff)", "VIP", "vanilla"): FIFO_LEFT_OFFSET,
    ("K=4 (cliff)", "BG", "cache_aware"): FIFO_LEFT_OFFSET,
    ("K=4 (cliff)", "BG", "vanilla"): FIFO_LEFT_OFFSET,
}

Y_LIMS = {
    "K=4 (cliff)": (0.0, 60.0),
    "K=8 (safe)": (0.04, 10.0),
}

ROOT = Path(__file__).resolve().parents[1]
PAPER_DATA_PATH = ROOT / "paper_data" / "figures" / "fig_baseline_tradeoff.json"


def load_paper_data():
    if not PAPER_DATA_PATH.exists():
        return PANELS, Y_LIMS
    with PAPER_DATA_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)
    panels = data.get("panels", PANELS)
    y_lims_raw = data.get("y_lims", Y_LIMS)
    y_lims = {k: tuple(v) for k, v in y_lims_raw.items()}
    return panels, y_lims


def main():
    plt.rcParams.update({"font.size": 8})
    panels, y_lims = load_paper_data()
    fig, axes = plt.subplots(
        2,
        2,
        figsize=(7.2, 3.6),
        sharex="row",
        sharey=False,
    )

    color_cycle = cycle(plt.rcParams["axes.prop_cycle"].by_key()["color"])
    colors = {p: next(color_cycle) for p in POLICY_ORDER}

    for col, (title, data) in enumerate(panels.items()):
        axes[0, col].set_title(title, fontsize=8, fontweight="bold")
        for row, metric in enumerate(["VIP", "BG"]):
            ax = axes[row, col]
            ax.grid(True, which="both", linestyle=":", linewidth=0.5, alpha=0.6)
            ymin, ymax = y_lims[title]
            combine_cluster = title == "K=8 (safe)"
            ca_point = None
            cl_point = None
            fifo_point = None
            for policy in POLICY_ORDER:
                if policy not in data:
                    continue
                vip, bg, thr = data[policy]
                y_val = vip if metric == "VIP" else bg
                clipped = False
                clipped_low = False
                if metric == "BG" and policy == "cap_only" and y_val > ymax:
                    if y_val > ymax:
                        y_val = ymax
                        clipped = True
                if policy in CONFUNDED and y_val < 1.0:
                    y_val = 1.0
                    clipped_low = True
                size = size_from_thr(thr)
                color = colors[policy]
                if policy in CONFUNDED:
                    ax.scatter(
                        thr,
                        y_val,
                        s=size,
                        facecolors="none",
                        edgecolors=color,
                        linewidths=1.1,
                        zorder=3,
                    )
                else:
                    ax.scatter(
                        thr,
                        y_val,
                        s=size,
                        color=color,
                        edgecolors="white",
                        linewidths=0.5,
                        zorder=3,
                    )
                if combine_cluster and policy in {"cache_aware", "gate_rr", "vanilla"}:
                    if policy == "cache_aware":
                        ca_point = (thr, y_val)
                    elif policy == "gate_rr":
                        cl_point = (thr, y_val)
                    else:
                        fifo_point = (thr, y_val)
                elif not (metric == "BG" and policy == "cap_only" and clipped):
                    # Place labels to the left-up for consistency.
                    dx, dy = (-18, 8)
                    size_scale = size / size_from_thr(10.66)
                    size_scale = max(0.6, min(1.0, size_scale))
                    size_scale = min(1.05, size_scale * 1.05)
                    dx *= size_scale
                    dy *= size_scale
                    if policy == "no_switch":
                        dx += LOCK_DELTA[0]
                        dy += LOCK_DELTA[1]
                    override = LABEL_OVERRIDES.get((title, metric, policy))
                    if override:
                        dx, dy = override
                    ax.annotate(
                        POLICY_LABEL[policy],
                        (thr, y_val),
                        xytext=(dx, dy),
                        textcoords="offset points",
                        fontsize=7,
                        fontweight="bold" if policy == "gate_rr" else "normal",
                    )
                if clipped:
                    ax.annotate(
                        "BGCap=118s (clipped)",
                        xy=(thr, y_val),
                        xytext=(-42, -12),
                        textcoords="offset points",
                        fontsize=7,
                        arrowprops=dict(arrowstyle="->", lw=0.7, color=color),
                    )
            if combine_cluster and any([ca_point, cl_point, fifo_point]):
                points = [p for p in (ca_point, cl_point, fifo_point) if p is not None]
                thr = points[0][0]
                y_val = max(p[1] for p in points)
                dx, dy = (-77, 8)
                ax.annotate(
                    "CLIMB",
                    (thr, y_val),
                    xytext=(dx, dy),
                    textcoords="offset points",
                    fontsize=7,
                    fontweight="bold",
                )
                ax.annotate(
                    " & LRU & FIFO",
                    (thr, y_val),
                    xytext=(dx + 22, dy),
                    textcoords="offset points",
                    fontsize=7,
                )
            ax.set_xlim(0, 11.2)
            ax.set_ylim(ymin, ymax)

    axes[0, 0].set_ylabel("VIP TTFT p99 (s)")
    axes[1, 0].set_ylabel("BG TTFT p99 (s)")
    for col in range(2):
        axes[1, col].set_xlabel("Throughput (rps)")

    fig.tight_layout()
    out_pdf = "figures/fig_baseline_tradeoff.pdf"
    fig.savefig(out_pdf)
    print(f"Wrote {out_pdf}")


if __name__ == "__main__":
    main()
