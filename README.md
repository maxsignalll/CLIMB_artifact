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
- `scripts/`: optional local GPU rerun, synthetic LoRA, and report helpers.
- `tools/`: small export utilities.

Policy mapping:

Main paper policies:

- `policies/gate_rr.py`: **CLIMB**
- `policies/vanilla.py`: **GlobalFIFO**
- `policies/cap_only.py`: **BGCap**
- `policies/cache_aware.py`: **LRUGate**
- `policies/no_switch.py`: **LockGate**

Appendix design-space variants:

- `policies/gate_rr_pp.py`: **ClassDRR**
- `policies/gate_u.py`: **UrgencyGate**
- `policies/gate_mix.py`: **SkewMixGate**
- `policies/legacy.py`: historical gate/DRR/stability variants used for
  appendix auditability

Support files:

- `policies/base.py`: shared base class
- `policies/__init__.py`: policy registry

## Quick Start: Regenerate Figures and Tables

These commands use `paper_data/` and do not require a GPU or raw logs. Outputs
are written to `figures/` and `results/summary/`.

```bash
python -m pip install -r requirements.txt

python scripts/check_artifact.py

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

The no-GPU path above is the recommended artifact check. The optional GPU path
is a mechanism sanity rerun for the W2/M8 residency-cliff setting; it is not
required to regenerate the checked-in figures and tables.

The public rerun uses synthetic LoRA adapters to recreate adapter residency
pressure. These adapters are not trained task adapters and are not committed to
the repository.

First install the optional serving dependencies:

```bash
python -m pip install -r requirements-gpu.txt
```

Prepare rank-128 q/k/v/o LoRA adapters outside the repository:

```bash
python scripts/prepare_synthetic_loras.py \
  --model /path/to/Qwen2.5-7B-Instruct \
  --out /path/to/climb_lora_adapters_r128_qkvo \
  --names vip,bg01,bg02,bg03,bg04,bg05,bg06,bg07 \
  --rank 128 \
  --target-modules q_proj,k_proj,v_proj,o_proj

du -sh /path/to/climb_lora_adapters_r128_qkvo/vip
```

Each generated adapter should be roughly 0.30 GiB for the Qwen2.5-7B q/k/v/o
rank-128 setting. Then edit the local paths in `configs/local_settings.sh`:

```bash
sed -n '1,120p' configs/local_settings.sh
```

Set at least:

- `VLLM_MODEL`: local base-model path.
- `LORA_DIR`: the synthetic adapter directory created above.
- `VLLM_ENV`: optional conda environment name for vLLM.

The default public sanity rerun compares `GlobalFIFO` and `CLIMB` at `K=4` for
the W2/M8 workload under open-loop load. The wrapper expects access to the full
experiment serving harness that provides `scripts/autodl/run_wk_sweep.sh`; set
`HARNESS_ROOT` to that checkout before running:

```bash
export HARNESS_ROOT=/path/to/full/experiment/checkout
bash scripts/run_gpu_w2_min_local.sh
```

To also run the safe-residency anchor at `K=8`, use:

```bash
KS="4 8" bash scripts/run_gpu_w2_min_local.sh
```

For each `K`, the wrapper passes `--vllm-max-loras K` to the harness so the
vLLM adapter-residency capacity matches the paper's budget axis.

The wrapper writes a short report to `reports/gpu_run_summary.md`, mirrors run
directories to `runs/`, and mirrors report CSVs to `report/`.

Expected mechanism-level outcome:

- `vanilla` is `GlobalFIFO`; under W2/M8 with `K=4`, VIP requests should show a
  large engine-side tail from adapter residency contention.
- `gate_rr` is `CLIMB`; it should reduce the VIP engine-side tail by shaping
  adapter residency, while residual end-to-end TTFT may still include ingress
  queueing under open-loop overload.
- Exact p99 values depend on the GPU, vLLM version, driver stack, and local
  serving harness. Treat this path as a sanity rerun, not a byte-for-byte replay
  of the authors' private raw logs.

Do not commit generated models, LoRA adapters, `runs/`, `report/`, `reports/`,
or local server logs.

## Citation

If this artifact is useful, please cite the CLIMB paper. A machine-readable
citation entry is provided in `CITATION.cff`.
