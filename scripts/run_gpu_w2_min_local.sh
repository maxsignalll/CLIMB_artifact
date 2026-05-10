#!/usr/bin/env bash
set -euo pipefail

# Wrapper for the canonical public GPU sanity rerun.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ARTIFACT_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
DEFAULT_HARNESS_ROOT=$(cd "${ARTIFACT_ROOT}/.." && pwd)
HARNESS_ROOT=${HARNESS_ROOT:-"${DEFAULT_HARNESS_ROOT}"}

CONFIG_PATH=${CONFIG_PATH:-"${ARTIFACT_ROOT}/configs/local_settings.sh"}
KS=${KS:-"4"}
RUN_SEED=${RUN_SEED:-101}
WORKLOAD=${WORKLOAD:-W2_phase_hol_rps3_p2048_split_M8}
POLICIES=${POLICIES:-"vanilla,gate_rr"}
EXP_PREFIX=${EXP_PREFIX:-hol_Ksweep_M8}
ARTIFACT_RUNS="${ARTIFACT_ROOT}/runs"
ARTIFACT_REPORT="${ARTIFACT_ROOT}/report"

if [[ ! -f "${CONFIG_PATH}" ]]; then
  echo "Config not found: ${CONFIG_PATH}" >&2
  exit 1
fi
if [[ ! -f "${HARNESS_ROOT}/scripts/autodl/run_wk_sweep.sh" ]]; then
  echo "Missing serving harness: ${HARNESS_ROOT}/scripts/autodl/run_wk_sweep.sh" >&2
  echo "Set HARNESS_ROOT=/path/to/full/experiment/checkout before running this wrapper." >&2
  echo "The no-GPU figure/table reproduction path does not require this harness." >&2
  exit 1
fi

cd "${HARNESS_ROOT}"

# shellcheck source=/dev/null
source "${CONFIG_PATH}"
VLLM_MAX_CPU_LORAS=${VLLM_MAX_CPU_LORAS:-8}
if [[ "${VLLM_MODEL:-}" == "/path/to/model" || ! -e "${VLLM_MODEL:-}" ]]; then
  echo "Invalid VLLM_MODEL in ${CONFIG_PATH}: ${VLLM_MODEL:-unset}" >&2
  exit 1
fi
if [[ "${LORA_DIR:-}" == "/path/to/synthetic_lora_adapters_r128_qkvo" || ! -d "${LORA_DIR:-}" ]]; then
  echo "Invalid LORA_DIR in ${CONFIG_PATH}: ${LORA_DIR:-unset}" >&2
  echo "Generate synthetic rank-128 adapters with scripts/prepare_synthetic_loras.py first." >&2
  exit 1
fi

mkdir -p "${ARTIFACT_RUNS}" "${ARTIFACT_REPORT}"

for k in ${KS}; do
  exp="${EXP_PREFIX}_k${k}"
  vllm_max_cpu_loras="${VLLM_MAX_CPU_LORAS}"
  if (( vllm_max_cpu_loras < k )); then
    vllm_max_cpu_loras="${k}"
  fi
  echo "[GPU] Running W2/M8 with K=${k}, seed=${RUN_SEED}, policies=${POLICIES}"
  bash scripts/autodl/run_wk_sweep.sh "${CONFIG_PATH}" \
    --exp "${exp}" \
    --workloads "${WORKLOAD}" \
    --policies "${POLICIES}" \
    --seed "${RUN_SEED}" \
    --k "${k}" \
    --vllm-max-loras "${k}" \
    --vllm-max-cpu-loras "${vllm_max_cpu_loras}"
  echo "[GPU] Done K=${k}"

  # Mirror runs/ and report/ into the artifact repository.
  if [[ -d "runs/${exp}" ]]; then
    if command -v rsync >/dev/null 2>&1; then
      rsync -a "runs/${exp}/" "${ARTIFACT_RUNS}/${exp}/"
    else
      mkdir -p "${ARTIFACT_RUNS}/${exp}"
      cp -a "runs/${exp}/." "${ARTIFACT_RUNS}/${exp}/"
    fi
  fi
  if [[ -f "report/${exp}_mp.csv" ]]; then
    cp -f "report/${exp}_mp.csv" "${ARTIFACT_REPORT}/"
  fi
done

python "${ARTIFACT_ROOT}/scripts/build_gpu_summary_md.py" \
  --report-dir "report" \
  --exp-prefix "${EXP_PREFIX}" \
  --ks "${KS}"

echo "Wrote reports/gpu_run_summary.md"
