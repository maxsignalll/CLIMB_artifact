# Local config template for the optional W2 GPU rerun.
# Edit paths before running scripts/run_gpu_w2_min_local.sh.
VLLM_MODEL="/path/to/model"
LORA_DIR="/path/to/synthetic_lora_adapters_r128_qkvo"
VLLM_ENV="env" # optional: conda env for vLLM

VLLM_HOST="127.0.0.1"
VLLM_PORT=8000
INGRESS_PORT=8001
LOG_DIR="${PWD}/logs_gpu"

# Canonical public sanity rerun: open-loop W2/M8 with rank-128 q/k/v/o LoRAs.
LOAD_MODE="open"
VLLM_MAX_MODEL_LEN=4096
VLLM_GPU_UTIL=0.85
VLLM_MAX_LORAS=4
VLLM_MAX_CPU_LORAS=8
VLLM_MAX_LORA_RANK=128
READY_TIMEOUT=1200

EXP_NAME="hol_Ksweep_M8"
SEEDS=(101)
