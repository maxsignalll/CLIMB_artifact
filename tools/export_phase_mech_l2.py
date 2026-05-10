#!/usr/bin/env python3
"""Export L2 time-series for fig_phase_mech from raw run dirs.

This uses the same computation as the original scripts/plot_phase_mech.py,
but writes a compact CSV under paper_data/figures/.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import importlib.util

import numpy as np
import pandas as pd


def _load_plot_module(repo_root: Path):
    mod_path = repo_root / "scripts" / "plot_phase_mech.py"
    spec = importlib.util.spec_from_file_location("plot_phase_mech", mod_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {mod_path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _thrash_to_grid(thrash, t_grid: np.ndarray) -> np.ndarray:
    if thrash is None:
        return np.zeros_like(t_grid, dtype=float)
    t, v = thrash
    return np.interp(t_grid, t, v, left=0.0, right=0.0)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--vanilla", type=Path, required=True, help="Run dir for GlobalFIFO.")
    parser.add_argument("--climb", type=Path, required=True, help="Run dir for CLIMB.")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("paper_data/figures/timeseries_phase_mech.csv"),
        help="Output CSV path (relative to the repository root).",
    )
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[2]
    mod = _load_plot_module(repo_root)

    van = mod._compute_series(args.vanilla)
    cli = mod._compute_series(args.climb)

    rows = []
    for name, data in [("GlobalFIFO", van), ("CLIMB", cli)]:
        t_grid = data["t_grid"]
        q = data["q_p99"]
        e = data["e_p99"]
        thrash = _thrash_to_grid(data.get("thrash"), t_grid)
        for t, qv, ev, tv in zip(t_grid, q, e, thrash):
            rows.append(
                {
                    "policy": name,
                    "t_s": float(t),
                    "queue_p99_s": float(qv) if np.isfinite(qv) else np.nan,
                    "engine_p99_s": float(ev) if np.isfinite(ev) else np.nan,
                    "thrash_per_s": float(tv) if np.isfinite(tv) else np.nan,
                }
            )

    df = pd.DataFrame(rows)
    out_path = Path(__file__).resolve().parents[1] / args.out
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path.as_posix()}")


if __name__ == "__main__":
    main()
