# Local safe config for W2 GPU mini-run (edit paths as needed)
VLLM_MODEL="/path/to/model"
LORA_DIR="/path/to/lora_cache"
VLLM_ENV="env" # optional: conda env for vLLM

VLLM_HOST="127.0.0.1"
VLLM_PORT=8000
INGRESS_PORT=8001
LOG_DIR="${PWD}/logs_gpu"

VLLM_MAX_MODEL_LEN=4096
VLLM_GPU_UTIL=0.85
VLLM_MAX_LORAS=4
VLLM_MAX_CPU_LORAS=64
VLLM_MAX_LORA_RANK=128
READY_TIMEOUT=180

EXP_NAME="hol_Ksweep_M8"
SEEDS=(101)
