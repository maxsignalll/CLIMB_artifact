#!/usr/bin/env python3
"""Run lightweight publication checks for the CLIMB artifact repository."""

from __future__ import annotations

import argparse
import csv
import importlib
import json
from pathlib import Path
import sys

sys.dont_write_bytecode = True


REQUIRED_PATHS = [
    "README.md",
    "LICENSE",
    "CITATION.cff",
    "requirements.txt",
    "requirements-gpu.txt",
    "paper_data/run_summaries.csv",
    "paper_data/results_snapshot/tables_snapshot.json",
    "paper_data/results_snapshot/curated_min.json",
    "paper_data/figures/fig_wk_sweep_combo.json",
    "paper_data/figures/fig_baseline_tradeoff.json",
    "paper_data/figures/timeseries_phase_mech.csv",
    "paper_data/figures/rank_sweep_heatmap.npz",
    "figures/fig_wk_sweep_combo.pdf",
    "figures/fig_baseline_tradeoff.pdf",
    "figures/fig_formula_diagnostic.pdf",
    "figures/fig_phase_mech.pdf",
    "figures/fig_rank_sweep_heatmap_w2.pdf",
    "results/summary/tab_cliff_safe.tex",
    "results/summary/tab_baseline_zoo.tex",
    "results/summary/tab_controls.tex",
    "results/summary/tab_pro6000_k4_m8.tex",
    "results/summary/tab_bg_liveness.tex",
    "scripts/prepare_synthetic_loras.py",
    "scripts/run_gpu_w2_min_local.sh",
]

EXPECTED_POLICIES = {
    "vanilla": "VanillaPolicy",
    "gate_rr": "GateRRPolicy",
    "cap_only": "CapOnlyPolicy",
    "cache_aware": "CacheAwarePolicy",
    "no_switch": "NoSwitchPolicy",
    "gate_rr_pp": "GateRRPPPolicy",
    "gate_u": "GateUPolicy",
    "gate_mix": "GateMixPolicy",
    "legacy": "LegacyPolicy",
}

RUN_SUMMARY_COLUMNS = {
    "run_id",
    "policy",
    "k",
    "seed",
    "workload_id",
    "table_id",
    "vip_ttft_p99_ms",
    "vip_queue_p99_ms",
    "vip_engine_p99_ms",
    "throughput_rps",
}
REQUIRED_TABLE_IDS = {"cliff_safe", "baseline_zoo", "controls", "pro6000", "bg_liveness"}
LEGACY_PHASE_SUMMARY_FIELDS = {"offered_bg_rps_each", "offered_vip_rps"}

FORBIDDEN_NAMES = {".DS_Store"}
FORBIDDEN_SUFFIXES = {".pyc", ".pyo", ".bak"}
FORBIDDEN_DIRS = {"__pycache__", ".pytest_cache", ".mypy_cache", "runs", "report", "reports", "logs_gpu", "tmp"}
LARGE_FILE_LIMIT_BYTES = 20 * 1024 * 1024


def ok(message: str) -> None:
    print(f"[OK] {message}")


def fail(errors: list[str], message: str) -> None:
    errors.append(message)
    print(f"[FAIL] {message}")


def check_required_paths(root: Path, errors: list[str]) -> None:
    missing = [path for path in REQUIRED_PATHS if not (root / path).exists()]
    if missing:
        fail(errors, "missing required paths: " + ", ".join(missing))
    else:
        ok("required files are present")


def check_json_inputs(root: Path, errors: list[str]) -> None:
    paths = [
        "paper_data/results_snapshot/tables_snapshot.json",
        "paper_data/results_snapshot/curated_min.json",
        "paper_data/figures/fig_wk_sweep_combo.json",
        "paper_data/figures/fig_baseline_tradeoff.json",
    ]
    bad = []
    for rel in paths:
        try:
            json.loads((root / rel).read_text(encoding="utf-8"))
        except Exception as exc:
            bad.append(f"{rel}: {exc}")
    if bad:
        fail(errors, "invalid JSON inputs: " + "; ".join(bad))
    else:
        ok("JSON inputs parse")


def check_run_summary(root: Path, errors: list[str]) -> None:
    path = root / "paper_data/run_summaries.csv"
    try:
        with path.open(newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            columns = set(reader.fieldnames or [])
            rows = list(reader)
    except Exception as exc:
        fail(errors, f"could not read {path.relative_to(root)}: {exc}")
        return

    missing_cols = sorted(RUN_SUMMARY_COLUMNS - columns)
    if missing_cols:
        fail(errors, "run_summaries.csv missing columns: " + ", ".join(missing_cols))
        return
    if not rows:
        fail(errors, "run_summaries.csv has no data rows")
        return

    policies = {row["policy"] for row in rows if row.get("policy")}
    if not {"vanilla", "gate_rr"}.issubset(policies):
        fail(errors, "run_summaries.csv must include vanilla and gate_rr rows")
        return
    table_ids = {row["table_id"] for row in rows if row.get("table_id")}
    missing_tables = sorted(REQUIRED_TABLE_IDS - table_ids)
    if missing_tables:
        fail(errors, "run_summaries.csv missing table_id values: " + ", ".join(missing_tables))
        return
    ok(f"run_summaries.csv has {len(rows)} rows, expected policies, and required table_ids")


def check_summary_metadata(root: Path, errors: list[str]) -> None:
    summary_root = root / "paper_data/summary"
    if not summary_root.exists():
        return
    bad = []
    for path in summary_root.rglob("summary.json"):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            bad.append(f"{path.relative_to(root)}: {exc}")
            continue
        stale = sorted(LEGACY_PHASE_SUMMARY_FIELDS & set(data))
        if stale:
            bad.append(f"{path.relative_to(root)}: stale scalar fields {', '.join(stale)}")
    if bad:
        fail(errors, "legacy summary metadata present: " + "; ".join(bad[:6]))
    else:
        ok("summary metadata has no stale scalar phase-load fields")


def check_policy_registry(root: Path, errors: list[str]) -> None:
    sys.path.insert(0, str(root))
    try:
        policies = importlib.import_module("policies")
    except Exception as exc:
        fail(errors, f"could not import policies package: {exc}")
        return
    registry = getattr(policies, "POLICY_REGISTRY", {})
    bad = []
    for key, class_name in EXPECTED_POLICIES.items():
        cls = registry.get(key)
        if cls is None:
            bad.append(f"{key}: missing")
        elif cls.__name__ != class_name:
            bad.append(f"{key}: expected {class_name}, got {cls.__name__}")
    if bad:
        fail(errors, "policy registry mismatch: " + "; ".join(bad))
    else:
        ok("policy registry imports with expected names")


def check_workload_and_readme(root: Path, errors: list[str]) -> None:
    workload = (root / "configs/workloads/W2_phase_hol_rps3_p2048_split_M8.yaml").read_text(
        encoding="utf-8"
    )
    missing_adapters = [name for name in ["vip", "bg01", "bg02", "bg03", "bg04", "bg05", "bg06", "bg07"] if name not in workload]
    if missing_adapters:
        fail(errors, "W2/M8 workload missing adapters: " + ", ".join(missing_adapters))
    else:
        ok("W2/M8 workload names match GPU rerun adapter set")

    readme = (root / "README.md").read_text(encoding="utf-8")
    needed = ["No-GPU", "Optional GPU Rerun", "prepare_synthetic_loras.py", "GlobalFIFO", "CLIMB"]
    missing_terms = [term for term in needed if term not in readme]
    if missing_terms:
        fail(errors, "README missing expected terms: " + ", ".join(missing_terms))
    else:
        ok("README contains no-GPU and GPU rerun guidance")


def check_clean_tree(root: Path, errors: list[str]) -> None:
    bad_paths = []
    large_files = []
    for path in root.rglob("*"):
        rel = path.relative_to(root)
        if ".git" in rel.parts:
            continue
        if any(part in FORBIDDEN_DIRS for part in rel.parts):
            bad_paths.append(str(rel))
            continue
        if path.name in FORBIDDEN_NAMES or path.suffix in FORBIDDEN_SUFFIXES:
            bad_paths.append(str(rel))
            continue
        if path.is_file() and path.stat().st_size > LARGE_FILE_LIMIT_BYTES:
            large_files.append(f"{rel} ({path.stat().st_size / (1024 * 1024):.1f} MiB)")

    if bad_paths:
        preview = ", ".join(sorted(bad_paths)[:12])
        extra = "" if len(bad_paths) <= 12 else f", ... +{len(bad_paths) - 12} more"
        fail(errors, "generated or local-only files present: " + preview + extra)
    else:
        ok("no cache, backup, local run, or tmp files found")

    if large_files:
        fail(errors, "large files exceed 20 MiB limit: " + ", ".join(large_files))
    else:
        ok("no file exceeds 20 MiB")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check CLIMB artifact readiness.")
    parser.add_argument("--root", default=Path(__file__).resolve().parents[1])
    args = parser.parse_args()

    root = Path(args.root).resolve()
    errors: list[str] = []
    print(f"Checking artifact root: {root}")
    check_required_paths(root, errors)
    check_json_inputs(root, errors)
    check_run_summary(root, errors)
    check_summary_metadata(root, errors)
    check_policy_registry(root, errors)
    check_workload_and_readme(root, errors)
    check_clean_tree(root, errors)

    if errors:
        print(f"\nARTIFACT_CHECK_FAILED ({len(errors)} issue(s))")
        return 1
    print("\nARTIFACT_CHECK_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
