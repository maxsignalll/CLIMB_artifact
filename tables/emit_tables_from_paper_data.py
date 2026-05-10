#!/usr/bin/env python3
"""Emit LaTeX tables from paper_data into results/summary.

This allows table regeneration without raw logs by copying the curated
paper_data/tables/*.tex outputs into results/summary/.
"""
from __future__ import annotations

from pathlib import Path
import shutil


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    src_dir = root / "paper_data" / "tables"
    out_dir = root / "results" / "summary"

    if not src_dir.exists():
        raise SystemExit("paper_data/tables not found. Nothing to emit.")

    out_dir.mkdir(parents=True, exist_ok=True)
    tex_files = sorted(src_dir.glob("*.tex"))
    if not tex_files:
        raise SystemExit("No .tex files found under paper_data/tables.")

    for tex in tex_files:
        dst = out_dir / tex.name
        shutil.copy2(tex, dst)
        print(f"Wrote results/summary/{tex.name}")


if __name__ == "__main__":
    main()
