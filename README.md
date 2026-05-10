# CLIMB Artifact

Artifact repository for **CLIMB: Taming the LoRA Residency Cliff in
Multi-LoRA Serving**.

This repository provides the public reproduction artifact for the paper. It is
designed to make the reported figures, tables, policy definitions, and
configuration choices inspectable without requiring access to the authors'
original raw logs or model weights.

## Reproducibility Scope

This repository supports two levels of reproduction.

1. **No-GPU artifact reproduction.** The curated data under `paper_data/`,
   together with the plotting and table scripts, regenerates the paper figures
   and result tables.
2. **Optional GPU reruns.** The repository includes workload configs, policy
   implementations, and a small local rerun wrapper. Full end-to-end reruns
   require local model weights, LoRA adapters, a compatible vLLM environment,
   and the serving harness used by the experimenter.

The repository intentionally excludes large raw logs and model weights. This
keeps the artifact compact while preserving the data needed to audit the paper
figures and tables.

## Repository Contents

- `figures/`: final paper figures in PDF/PNG form.
- `results/summary/`: LaTeX tables used by the paper.
- `paper_data/`: curated intermediate data used to regenerate figures and
  tables without raw logs.
- `plots/`: plotting scripts.
- `tables/`: table-generation scripts, including snapshot builders and renderers.
- `analysis/formula_fit_out/`: diagnostic windows, mirrored under
  `paper_data/`.
- `configs/`: workload and policy configuration files.
- `policies/`: policy implementations and the registry.
- `scripts/`: optional local GPU rerun/report helpers.
- `tools/`: small export utilities.

Policy mapping:

- `policies/gate_rr.py`: **CLIMB**
- `policies/vanilla.py`: **GlobalFIFO**
- `policies/cap_only.py`: **BGCap**
- `policies/cache_aware.py`: **LRUGate**
- `policies/no_switch.py`: **LockGate**
- `policies/gate_rr_pp.py`: **ClassDRR**
- `policies/gate_u.py`: **UrgencyGate**
- `policies/gate_mix.py`: **SkewMixGate**
- `policies/legacy.py`: historical variants used for auditability
- `policies/base.py`: shared base class
- `policies/__init__.py`: policy registry

## Quick Start: Regenerate Figures and Tables

These commands use `paper_data/` and do not require a GPU or raw logs. Outputs
are written to `figures/` and `results/summary/`.

```bash
python -m pip install -r requirements.txt

python plots/plot_wk_sweep_combo.py
python plots/plot_baseline_tradeoff.py
python plots/plot_formula_diagnostic.py
python plots/plot_phase_mech.py --paper-data
python plots/plot_rank_sweep_heatmap.py --paper-data

python tables/build_per_class_throughput_tok_eq.py
python tables/build_vip_absence_table.py
python tables/build_snapshot_from_run_summaries.py
python tables/render_snapshot_tables.py
```

Notes:

- `plot_formula_diagnostic.py` uses `paper_data/diagnostic/` when present.
- `plot_wk_sweep_combo.py` uses
  `paper_data/figures/fig_wk_sweep_combo.json`.
- `plot_baseline_tradeoff.py` uses
  `paper_data/figures/fig_baseline_tradeoff.json`.
- `plot_phase_mech.py --paper-data` uses
  `paper_data/figures/timeseries_phase_mech.csv`.
- `plot_rank_sweep_heatmap.py --paper-data` uses
  `paper_data/figures/rank_sweep_heatmap.npz`.
- `build_snapshot_from_run_summaries.py` builds
  `paper_data/results_snapshot/` from `paper_data/run_summaries.csv`.
- `render_snapshot_tables.py` renders tables from
  `paper_data/results_snapshot/`.

## Figure Provenance

Generated from `paper_data/`:

- `fig_wk_sweep_combo.pdf`
  (`paper_data/figures/fig_wk_sweep_combo.json`)
- `fig_baseline_tradeoff.pdf`
  (`paper_data/figures/fig_baseline_tradeoff.json`)
- `fig_formula_diagnostic.pdf` and
  `fig_formula_diagnostic_appendix.pdf`
  (`paper_data/diagnostic/**/windows.csv`)
- `fig_phase_mech.pdf`
  (`paper_data/figures/timeseries_phase_mech.csv`)
- `fig_rank_sweep_heatmap_w2.pdf`
  (`paper_data/figures/rank_sweep_heatmap.npz`)

## Table Provenance

Generated directly from `paper_data/`:

- `tab_per_class_throughput_tok_eq.tex`
  (`tables/build_per_class_throughput_tok_eq.py`)
- `tab_vip_absence.tex`
  (`tables/build_vip_absence_table.py`)

Rendered from `paper_data/results_snapshot/`, which can be generated from
`paper_data/run_summaries.csv`:

- `tab_cliff_safe.tex`
- `tab_baseline_zoo.tex`
- `tab_controls.tex`
- `tab_pro6000_k4_m8.tex`
- `tab_bg_liveness.tex`

Rendered from curated snapshots in
`paper_data/results_snapshot/curated_min.json`:

- `tab_overhead_appendix.tex`
- `tab_variant_mechanisms.tex`
- `tab_gaterrpp_wtl.tex`

Some narrative or definition tables live directly in the paper TeX and are not
part of `results/summary/`. The diagnostic AUC table is also in the paper PDF;
this artifact provides the corresponding AUC plot inputs and plotting script.

## Optional GPU Rerun

The no-GPU path above is the recommended artifact check. For a local GPU sanity
rerun, first install the optional serving dependencies:

```bash
python -m pip install -r requirements-gpu.txt
```

Then edit the local paths in `configs/local_settings.sh`:

```bash
sed -n '1,120p' configs/local_settings.sh
```

The included wrapper runs `GlobalFIFO` and `CLIMB` once each at `K=4` and `K=8`
for the W2/M8 workload. The wrapper expects access to the full experiment
serving harness that provides `scripts/autodl/run_wk_sweep.sh`; set
`HARNESS_ROOT` to that checkout before running it:

```bash
export HARNESS_ROOT=/path/to/full/experiment/checkout
bash scripts/run_gpu_w2_min_local.sh
```

The wrapper writes a short report to `reports/gpu_run_summary.md`, mirrors run
directories to `runs/`, and mirrors report CSVs to `report/`.

## Citation

If this artifact is useful, please cite the CLIMB paper. A machine-readable
citation entry is provided in `CITATION.cff`.
