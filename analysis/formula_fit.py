#!/usr/bin/env python3
import argparse
import csv
import glob
import json
import math
import os
from collections import defaultdict


def parse_args():
    p = argparse.ArgumentParser(description="Window-level diagnostic fit for cliff heuristic.")
    p.add_argument(
        "--runs",
        action="append",
        required=True,
        help="Run dir or glob (can be repeated).",
    )
    p.add_argument("--safe-run", required=True, help="Safe anchor run dir or glob.")
    p.add_argument("--window-s", type=float, default=10.0)
    p.add_argument("--vip-stat", choices=["p90", "p95", "p99", "mean", "cvar99"], default="p95")
    p.add_argument("--bad-gamma", type=float, default=5.0)
    p.add_argument("--bad-abs-ms", type=float, default=None)
    p.add_argument("--min-vip", type=int, default=10)
    p.add_argument("--tau-min", type=float, default=0.0)
    p.add_argument("--tau-max", type=float, default=200.0)
    p.add_argument("--tau-step", type=float, default=5.0)
    p.add_argument("--bins", type=int, default=10)
    p.add_argument("--out-dir", default="analysis/formula_fit_out")
    return p.parse_args()


def resolve_glob(path):
    if any(ch in path for ch in "*?[]"):
        matches = sorted(glob.glob(path))
        if not matches:
            raise FileNotFoundError(f"No matches for {path}")
        return matches
    return [path]


def resolve_requests_log(run_dir):
    if os.path.isfile(run_dir) and run_dir.endswith("requests_log.csv"):
        return run_dir
    candidate = os.path.join(run_dir, "requests_log.csv")
    if os.path.exists(candidate):
        return candidate
    raise FileNotFoundError(f"requests_log.csv not found under {run_dir}")


def resolve_summary(run_dir):
    if os.path.isfile(run_dir) and run_dir.endswith("summary.json"):
        return run_dir
    candidate = os.path.join(run_dir, "summary.json")
    if os.path.exists(candidate):
        return candidate
    raise FileNotFoundError(f"summary.json not found under {run_dir}")


def percentile(vals, p):
    if not vals:
        return None
    vals = sorted(vals)
    idx = int(math.ceil(p * len(vals))) - 1
    idx = max(0, min(idx, len(vals) - 1))
    return vals[idx]


def cvar(vals, alpha=0.99):
    if not vals:
        return None
    vals = sorted(vals)
    n = len(vals)
    k = max(1, int(math.ceil(n * (1 - alpha))))
    return sum(vals[-k:]) / float(k)


def compute_stat(vals, stat):
    if not vals:
        return None
    if stat == "mean":
        return sum(vals) / float(len(vals))
    if stat == "p90":
        return percentile(vals, 0.90)
    if stat == "p95":
        return percentile(vals, 0.95)
    if stat == "p99":
        return percentile(vals, 0.99)
    if stat == "cvar99":
        return cvar(vals, 0.99)
    raise ValueError(stat)


def load_safe_s0(safe_run_dir):
    req_path = resolve_requests_log(safe_run_dir)
    vip_engine = []
    with open(req_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("class") != "VIP":
                continue
            if row.get("ok") not in ("True", "true", True):
                continue
            try:
                vip_engine.append(float(row["engine_ms"]))
            except Exception:
                pass
    if not vip_engine:
        raise ValueError("No VIP samples in safe run.")
    s0_ms = percentile(vip_engine, 0.50)
    return s0_ms / 1000.0


def window_stats(run_dir, window_s, vip_stat, min_vip):
    req_path = resolve_requests_log(run_dir)
    sum_path = resolve_summary(run_dir)
    with open(sum_path, "r") as f:
        summary = json.load(f)
    k = summary.get("k")
    run_id = summary.get("run_id", os.path.basename(run_dir))

    windows = defaultdict(lambda: {"count": 0, "vip_engine": [], "adapters": set()})
    t0 = None
    with open(req_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                arrival = float(row["arrival_ts"])
            except Exception:
                continue
            if t0 is None or arrival < t0:
                t0 = arrival
            win = int((arrival - t0) // window_s)
            w = windows[win]
            w["count"] += 1
            adapter = row.get("adapter_id")
            if adapter:
                w["adapters"].add(adapter)
            if row.get("class") == "VIP" and row.get("ok") in ("True", "true", True):
                try:
                    w["vip_engine"].append(float(row["engine_ms"]) / 1000.0)
                except Exception:
                    pass

    out = []
    for win, w in windows.items():
        if len(w["vip_engine"]) < min_vip:
            continue
        vip_val = compute_stat(w["vip_engine"], vip_stat)
        if vip_val is None:
            continue
        w_window = len(w["adapters"])
        if "vip" not in w["adapters"]:
            w_window += 1
        if w_window <= 0:
            continue
        p_miss = max(0.0, 1.0 - (k / float(w_window)))
        lam = w["count"] / float(window_s)
        out.append(
            {
                "run_id": run_id,
                "k": k,
                "window": win,
                "lambda": lam,
                "w_window": w_window,
                "p_miss": p_miss,
                "vip_stat": vip_val,
            }
        )
    return out


def auc_score(xs, ys):
    # Mann–Whitney U AUC
    paired = list(zip(xs, ys))
    paired.sort(key=lambda x: x[0])
    ranks = []
    i = 0
    n = len(paired)
    while i < n:
        j = i
        while j + 1 < n and paired[j + 1][0] == paired[i][0]:
            j += 1
        avg_rank = (i + j + 2) / 2.0
        for k in range(i, j + 1):
            ranks.append((paired[k][1], avg_rank))
        i = j + 1
    rank_sum_bad = sum(r for y, r in ranks if y == 1)
    n_bad = sum(1 for y, _ in ranks if y == 1)
    n_good = n - n_bad
    if n_bad == 0 or n_good == 0:
        return 0.5
    u = rank_sum_bad - n_bad * (n_bad + 1) / 2.0
    return u / float(n_bad * n_good)


def main():
    args = parse_args()
    safe_matches = resolve_glob(args.safe_run)
    safe_run = safe_matches[0]
    s0 = load_safe_s0(safe_run)
    bad_abs_s = args.bad_abs_ms / 1000.0 if args.bad_abs_ms else None

    run_dirs = []
    for r in args.runs:
        run_dirs.extend(resolve_glob(r))
    run_dirs = sorted(set(run_dirs))

    windows = []
    for run_dir in run_dirs:
        try:
            windows.extend(window_stats(run_dir, args.window_s, args.vip_stat, args.min_vip))
        except FileNotFoundError:
            continue

    if not windows:
        raise SystemExit("No windows collected.")

    for w in windows:
        thresh = args.bad_gamma * s0
        if bad_abs_s is not None:
            thresh = max(thresh, bad_abs_s)
        w["bad"] = 1 if w["vip_stat"] > thresh else 0

    tau_vals = []
    t = args.tau_min
    while t <= args.tau_max + 1e-9:
        tau_vals.append(round(t, 6))
        t += args.tau_step

    best = {"tau": None, "auc": -1.0}
    for tau in tau_vals:
        xs = []
        ys = []
        for w in windows:
            rho = w["lambda"] * (s0 + w["p_miss"] * tau)
            xs.append(rho)
            ys.append(w["bad"])
        auc = auc_score(xs, ys)
        if auc > best["auc"]:
            best = {"tau": tau, "auc": auc}

    # compute rho with best tau
    tau = best["tau"]
    for w in windows:
        w["rho_eff"] = w["lambda"] * (s0 + w["p_miss"] * tau)

    os.makedirs(args.out_dir, exist_ok=True)
    win_path = os.path.join(args.out_dir, "windows.csv")
    with open(win_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["run_id", "k", "window", "lambda", "w_window", "p_miss", "vip_stat", "bad", "rho_eff"]
        )
        for w in windows:
            writer.writerow(
                [
                    w["run_id"],
                    w["k"],
                    w["window"],
                    f"{w['lambda']:.6f}",
                    w["w_window"],
                    f"{w['p_miss']:.6f}",
                    f"{w['vip_stat']:.6f}",
                    w["bad"],
                    f"{w['rho_eff']:.6f}",
                ]
            )

    # binned bad rate
    rhos = [w["rho_eff"] for w in windows]
    rmin, rmax = min(rhos), max(rhos)
    if rmax == rmin:
        rmax = rmin + 1e-6
    bin_edges = [rmin + i * (rmax - rmin) / args.bins for i in range(args.bins + 1)]
    bins = [{"count": 0, "bad": 0} for _ in range(args.bins)]
    for w in windows:
        rho = w["rho_eff"]
        idx = min(args.bins - 1, int((rho - rmin) / (rmax - rmin) * args.bins))
        bins[idx]["count"] += 1
        bins[idx]["bad"] += w["bad"]

    bin_path = os.path.join(args.out_dir, "binned_bad_rate.csv")
    with open(bin_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["bin_lo", "bin_hi", "bin_mid", "count", "bad_rate"])
        for i, b in enumerate(bins):
            lo, hi = bin_edges[i], bin_edges[i + 1]
            mid = (lo + hi) / 2.0
            rate = (b["bad"] / b["count"]) if b["count"] else 0.0
            writer.writerow([f"{lo:.6f}", f"{hi:.6f}", f"{mid:.6f}", b["count"], f"{rate:.6f}"])

    print(f"s0_s={s0:.4f}")
    print(f"best_tau_s={best['tau']:.2f} auc={best['auc']:.3f}")
    print(f"windows={len(windows)} out_dir={args.out_dir}")


if __name__ == "__main__":
    main()
