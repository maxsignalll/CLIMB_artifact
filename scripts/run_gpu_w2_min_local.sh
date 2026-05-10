#!/usr/bin/env bash
set -euo pipefail

# Wrapper for a small local GPU rerun.
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ARTIFACT_ROOT=$(cd "${SCRIPT_DIR}/.." && pwd)
DEFAULT_HARNESS_ROOT=$(cd "${ARTIFACT_ROOT}/.." && pwd)
HARNESS_ROOT=${HARNESS_ROOT:-"${DEFAULT_HARNESS_ROOT}"}

CONFIG_PATH=${CONFIG_PATH:-"${ARTIFACT_ROOT}/configs/local_settings.sh"}
KS=${KS:-"4 8"}
WORKLOAD=${WORKLOAD:-W2_phase_hol_rps3_p2048_split_M8}
POLICIES=${POLICIES:-"vanilla,gate_rr"}
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

mkdir -p "${ARTIFACT_RUNS}" "${ARTIFACT_REPORT}"

for k in ${KS}; do
  echo "[GPU] Running W2/M8 with K=${k}, policies=${POLICIES}"
  bash scripts/autodl/run_wk_sweep.sh "${CONFIG_PATH}" \
    --exp "hol_Ksweep_M8_k${k}" \
    --workloads "${WORKLOAD}" \
    --policies "${POLICIES}" \
    --k "${k}"
  echo "[GPU] Done K=${k}"

  # Mirror runs/ and report/ into the artifact repository.
  if [[ -d "runs/hol_Ksweep_M8_k${k}" ]]; then
    if command -v rsync >/dev/null 2>&1; then
      rsync -a "runs/hol_Ksweep_M8_k${k}/" "${ARTIFACT_RUNS}/hol_Ksweep_M8_k${k}/"
    else
      mkdir -p "${ARTIFACT_RUNS}/hol_Ksweep_M8_k${k}"
      cp -a "runs/hol_Ksweep_M8_k${k}/." "${ARTIFACT_RUNS}/hol_Ksweep_M8_k${k}/"
    fi
  fi
  if [[ -f "report/hol_Ksweep_M8_k${k}_mp.csv" ]]; then
    cp -f "report/hol_Ksweep_M8_k${k}_mp.csv" "${ARTIFACT_REPORT}/"
  fi
 done

python "${ARTIFACT_ROOT}/scripts/build_gpu_summary_md.py" --report-dir "report"

echo "Wrote reports/gpu_run_summary.md"
