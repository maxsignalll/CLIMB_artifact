#!/usr/bin/env python3
import argparse
import glob
from pathlib import Path

import numpy as np
import pandas as pd

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

COLOR_BASE = "#0072B2"  # Okabe-Ito blue (vanilla palette)
COLOR_CLIMB = "#D55E00"  # Okabe-Ito vermilion (CLIMB palette)


def roc_curve_np(y_true, scores):
    order = np.argsort(scores)[::-1]
    y = y_true[order]
    s = scores[order]
    distinct = np.where(np.diff(s))[0]
    idxs = np.r_[distinct, y.size - 1]
    tps = np.cumsum(y)[idxs]
    fps = np.cumsum(1 - y)[idxs]
    tps = np.r_[0, tps]
    fps = np.r_[0, fps]
    pos = y.sum()
    neg = y.size - pos
    tpr = tps / pos if pos > 0 else np.zeros_like(tps)
    fpr = fps / neg if neg > 0 else np.zeros_like(fps)
    thr = np.r_[np.inf, s[idxs]]
    return fpr, tpr, thr


def auc_np(fpr, tpr):
    return np.trapezoid(tpr, fpr)


def roc_and_auc(y_true, scores):
    try:
        from sklearn.metrics import roc_curve, auc  # type: ignore

        fpr, tpr, _ = roc_curve(y_true, scores, pos_label=1)
        return fpr, tpr, auc(fpr, tpr)
    except Exception:
        fpr, tpr, _ = roc_curve_np(y_true, scores)
        return fpr, tpr, auc_np(fpr, tpr)


def load_windows(path):
    df = pd.read_csv(path)
    df["bad"] = df["bad"].astype(int)
    return df


def estimate_s0_from_tau0(df_tau0):
    lam = df_tau0["lambda"].astype(float).to_numpy()
    rho = df_tau0["rho_eff"].astype(float).to_numpy()
    mask = lam > 0
    if not mask.any():
        raise ValueError("No positive lambda to estimate s0.")
    s0_vals = rho[mask] / lam[mask]
    return float(np.median(s0_vals))


def compute_rho(df, s0, tau):
    lam = df["lambda"].astype(float).to_numpy()
    p_miss = df["p_miss"].astype(float).to_numpy()
    return lam * (s0 + p_miss * tau)


def find_dataset(paths, key_substr, fallback="max"):
    cand = [p for p in paths if key_substr in p]
    if not cand:
        cand = paths[:]
    counts = []
    for p in cand:
        try:
            counts.append((sum(1 for _ in open(p)) - 1, p))
        except Exception:
            counts.append((0, p))
    if fallback == "min":
        counts.sort(key=lambda x: (x[0], x[1]))
    else:
        counts.sort(key=lambda x: (-x[0], x[1]))
    return counts[0][1]


def main():
    p = argparse.ArgumentParser(description="Plot formula diagnostic ROC/AUC figures.")
    p.add_argument(
        "--out-main",
        default="figures/fig_formula_diagnostic.pdf",
        help="Main ROC PDF output.",
    )
    p.add_argument(
        "--out-main-png",
        default="figures/fig_formula_diagnostic.png",
        help="Main ROC PNG output.",
    )
    p.add_argument(
        "--out-app",
        default="figures/fig_formula_diagnostic_appendix.pdf",
        help="Appendix PDF output.",
    )
    p.add_argument(
        "--out-app-png",
        default="figures/fig_formula_diagnostic_appendix.png",
        help="Appendix PNG output.",
    )
    p.add_argument(
        "--tau-step",
        type=float,
        default=5.0,
        help="Tau grid step for AUC-vs-tau panel.",
    )
    p.add_argument("--tau-max", type=float, default=200.0)
    p.add_argument("--tau0", type=float, default=0.0)
    p.add_argument("--tau1", type=float, default=5.0)
    args = p.parse_args()

    root = Path(__file__).resolve().parents[1]
    paper_root = root / "paper_data" / "diagnostic" / "formula_fit_out"
    paths = glob.glob(str(paper_root / "**/windows.csv"), recursive=True)
    if not paths:
        paths = glob.glob(str(root / "analysis" / "formula_fit_out" / "**/windows.csv"), recursive=True)
    if not paths:
        raise SystemExit("No windows.csv found under paper_data/diagnostic/formula_fit_out or analysis/formula_fit_out.")

    ksweep_path = find_dataset(paths, "ksweep", fallback="max")
    ksweep_tau0_path = find_dataset(paths, "ksweep_tau0", fallback="max")
    k7k8_path = find_dataset(paths, "k7k8", fallback="min")
    k7k8_tau0_path = find_dataset(paths, "k7k8_tau0", fallback="min")

    ksweep_df = load_windows(ksweep_path)
    ksweep_tau0_df = load_windows(ksweep_tau0_path)
    k7k8_df = load_windows(k7k8_path)
    k7k8_tau0_df = load_windows(k7k8_tau0_path)

    s0 = estimate_s0_from_tau0(ksweep_tau0_df)
    s0_k7k8 = estimate_s0_from_tau0(k7k8_tau0_df)

    rho0 = compute_rho(ksweep_df, s0, args.tau0)
    rho5 = compute_rho(ksweep_df, s0, args.tau1)
    y = ksweep_df["bad"].to_numpy()
    fpr0, tpr0, auc0 = roc_and_auc(y, rho0)
    fpr5, tpr5, auc5 = roc_and_auc(y, rho5)

    # Main figure (single-column ROC)
    plt.rcParams.update({"font.size": 8})
    fig, ax = plt.subplots(figsize=(3.4, 2.4))
    ax.plot(
        fpr0,
        tpr0,
        lw=1.2,
        color=COLOR_BASE,
        label=f"tau=0 (AUC={auc0:.3f})",
    )
    ax.plot(
        fpr5,
        tpr5,
        lw=1.2,
        color=COLOR_CLIMB,
        linestyle="--",
        label=f"tau=5 (AUC={auc5:.3f})",
    )
    ax.plot([0, 1], [0, 1], color="0.8", lw=0.8, ls="--")
    ax.set_xlabel("False positive rate")
    ax.set_ylabel("True positive rate")
    ax.set_xlim(-0.02, 1)
    ax.set_ylim(0, 1.02)
    ax.legend(fontsize=7, frameon=False)
    ax.grid(False)
    fig.tight_layout()
    fig.savefig(args.out_main)
    fig.savefig(args.out_main_png, dpi=300)

    # Appendix figure
    taus = np.arange(args.tau0, args.tau_max + 1e-9, args.tau_step)
    aucs = []
    for tau in taus:
        rho = compute_rho(ksweep_df, s0, tau)
        _, _, auc_tau = roc_and_auc(y, rho)
        aucs.append(auc_tau)

    y_k7k8 = k7k8_df["bad"].to_numpy()
    rho0_k7k8 = compute_rho(k7k8_df, s0_k7k8, args.tau0)
    rho5_k7k8 = compute_rho(k7k8_df, s0_k7k8, args.tau1)
    fpr0_k7k8, tpr0_k7k8, auc0_k7k8 = roc_and_auc(y_k7k8, rho0_k7k8)
    fpr5_k7k8, tpr5_k7k8, auc5_k7k8 = roc_and_auc(y_k7k8, rho5_k7k8)

    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.4))
    # (a) AUC vs tau
    ax = axes[0]
    ax.plot(taus, aucs, lw=1.2, color=COLOR_BASE)
    ax.scatter(
        [args.tau0],
        [aucs[0]],
        s=20,
        color=COLOR_BASE,
    )
    ax.scatter(
        [args.tau1],
        [aucs[int(args.tau1 / args.tau_step)]],
        s=20,
        color=COLOR_CLIMB,
    )
    ax.set_xlabel("tau (s)")
    ax.set_ylabel("AUC")
    ax.set_ylim(0.4, 1.02)
    ax.grid(True, linestyle=":", linewidth=0.5, alpha=0.6)
    ax.set_title("K-sweep AUC vs tau", fontsize=8)

    # (b) ROC for K-sweep
    ax = axes[1]
    ax.plot(
        fpr0,
        tpr0,
        lw=1.2,
        color=COLOR_BASE,
        label=f"tau=0 (AUC={auc0:.3f})",
    )
    ax.plot(
        fpr5,
        tpr5,
        lw=1.2,
        color=COLOR_CLIMB,
        linestyle="--",
        label=f"tau=5 (AUC={auc5:.3f})",
    )
    ax.plot([0, 1], [0, 1], color="0.8", lw=0.8, ls="--")
    ax.set_xlabel("FPR")
    ax.set_ylabel("TPR")
    ax.set_xlim(-0.02, 1)
    ax.set_ylim(0, 1.02)
    ax.legend(fontsize=7, frameon=False)
    ax.set_title("K-sweep ROC", fontsize=8)

    # (c) ROC for same-lambda (K=8 vs K=7)
    ax = axes[2]
    ax.plot(
        fpr0_k7k8,
        tpr0_k7k8,
        lw=1.2,
        color=COLOR_BASE,
        label=f"tau=0 (AUC={auc0_k7k8:.3f})",
    )
    ax.plot(
        fpr5_k7k8,
        tpr5_k7k8,
        lw=1.2,
        color=COLOR_CLIMB,
        linestyle="--",
        label=f"tau=5 (AUC={auc5_k7k8:.3f})",
    )
    ax.plot([0, 1], [0, 1], color="0.8", lw=0.8, ls="--")
    ax.set_xlabel("FPR")
    ax.set_ylabel("TPR")
    ax.set_xlim(-0.02, 1)
    ax.set_ylim(0, 1.02)
    ax.legend(fontsize=7, frameon=False)
    ax.set_title("Same-λ ROC", fontsize=8)

    for ax in axes:
        ax.grid(False)

    fig.tight_layout()
    fig.savefig(args.out_app)
    fig.savefig(args.out_app_png, dpi=300)

    print(f"Main ROC: tau=0 AUC={auc0:.3f}, tau=5 AUC={auc5:.3f}")
    print(f"Same-λ ROC: tau=0 AUC={auc0_k7k8:.3f}, tau=5 AUC={auc5_k7k8:.3f}")
    print(f"Outputs: {args.out_main}, {args.out_main_png}, {args.out_app}, {args.out_app_png}")


if __name__ == "__main__":
    main()
