#!/usr/bin/env bash
set -euo pipefail

# Default parameters
PORT="8011"
HOST="0.0.0.0"
MODEL="Qwen/Qwen3-4B"
MAX_MODEL_LEN="8000"
KILL_PORT="0"  # 1=true, 0=false
GPU_MEMORY_UTILIZATION="0.5"   # fraction of per-GPU memory to use
CUDA_VISIBLE_DEVICES_ARG=""     # e.g., "0" or "0,1"
TENSOR_PARALLEL_SIZE=""         # e.g., 1, 2, ...

usage() {
  cat <<EOF
Usage: $0 [--port N] [--host HOST] [--model NAME] [--max-model-len N] [--no-kill]
         [--cuda-visible-devices LIST] [--gpu-memory-utilization F] 
         [--tensor-parallel-size N] [-h|--help]

Starts vLLM server.

Options:
  --port N            Port to bind (default: ${PORT})
  --host HOST         Host to bind (default: ${HOST})
  --model NAME        Model name or path (default: ${MODEL})
  --max-model-len N   Max model length (default: ${MAX_MODEL_LEN})
  --no-kill           Do not kill existing process on the port; fail if busy
  --cuda-visible-devices LIST   GPUs to use (e.g., "0" or "0,1"). If set, exports CUDA_VISIBLE_DEVICES
  --gpu-memory-utilization F    Fraction of per-GPU memory to use (default: ${GPU_MEMORY_UTILIZATION})
  --tensor-parallel-size N      Tensor parallel size across GPUs (default: unset -> 1)
  -h, --help          Show this help

Examples:
  $0                              # start on port 8011
  $0 --port 9000                  # start on port 9000
  $0 --no-kill                    # do not free the port automatically
EOF
}

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port)
      PORT="$2"; shift 2;;
    --host)
      HOST="$2"; shift 2;;
    --model)
      MODEL="$2"; shift 2;;
    --max-model-len)
      MAX_MODEL_LEN="$2"; shift 2;;
    --no-kill)
      KILL_PORT="0"; shift 1;;
    --cuda-visible-devices)
      CUDA_VISIBLE_DEVICES_ARG="$2"; shift 2;;
    --gpu-memory-utilization)
      GPU_MEMORY_UTILIZATION="$2"; shift 2;;
    --tensor-parallel-size)
      TENSOR_PARALLEL_SIZE="$2"; shift 2;;
    -h|--help)
      usage; exit 0;;
    *)
      echo "Unknown argument: $1" >&2; usage; exit 1;;
  esac
done

log() { echo "[run_vllm] $*"; }

# Ensure conda is available and activate vllm_env
if command -v conda >/dev/null 2>&1; then
  # shellcheck disable=SC1091
  eval "$(conda shell.bash hook)" || true
  if conda env list | awk '{print $1}' | grep -qx "vllm_env"; then
    conda activate vllm_env
    log "Activated conda env: vllm_env"
  else
    log "Conda env 'vllm_env' not found. Continuing without activation."
  fi
else
  # Try common locations if conda not on PATH
  if [[ -f "/opt/miniconda/etc/profile.d/conda.sh" ]]; then
    # shellcheck disable=SC1091
    source /opt/miniconda/etc/profile.d/conda.sh
    conda activate vllm_env || true
    log "Activated conda env via /opt/miniconda"
  else
    log "conda not found on PATH and /opt/miniconda not present; assuming vllm is available."
  fi
fi

# Check if port is busy
if ss -lnt "( sport = :${PORT} )" | grep -q ":${PORT} .*LISTEN"; then
  if [[ "${KILL_PORT}" == "1" ]]; then
    log "Port ${PORT} is busy; attempting to free it with fuser..."
    if command -v fuser >/dev/null 2>&1; then
      if [[ $EUID -ne 0 ]]; then
        # Try without sudo first; fallback to sudo
        fuser -k "${PORT}/tcp" || sudo fuser -k "${PORT}/tcp" || true
      else
        fuser -k "${PORT}/tcp" || true
      fi
      sleep 1
    else
      log "fuser not found; cannot auto-free port. Exiting."
      exit 1
    fi
  else
    log "Port ${PORT} is in use and --no-kill was specified. Exiting."; exit 1
  fi
fi

# Create logs directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="${SCRIPT_DIR}/../logs"
mkdir -p "${LOG_DIR}"
LOG_FILE="${LOG_DIR}/vllm_${PORT}.log"

# Start vLLM server
log "Starting vLLM: model='${MODEL}', host='${HOST}', port='${PORT}', max_model_len='${MAX_MODEL_LEN}'"
# Respect CUDA_VISIBLE_DEVICES if provided
if [[ -n "${CUDA_VISIBLE_DEVICES_ARG}" ]]; then
  export CUDA_VISIBLE_DEVICES="${CUDA_VISIBLE_DEVICES_ARG}"
  log "CUDA_VISIBLE_DEVICES='${CUDA_VISIBLE_DEVICES}'"
fi
CMD=(
  vllm serve "${MODEL}"
  --host "${HOST}"
  --port "${PORT}"
  --max-model-len "${MAX_MODEL_LEN}"
  --gpu-memory-utilization "${GPU_MEMORY_UTILIZATION}"
)
# Optional tensor parallel size
if [[ -n "${TENSOR_PARALLEL_SIZE}" ]]; then
  CMD+=( --tensor-parallel-size "${TENSOR_PARALLEL_SIZE}" )
fi

# Print and exec
log "Command: ${CMD[*]}"
# Run in foreground; use a separate terminal or append '&' if you want background.
"${CMD[@]}" 2>&1 | tee -a "${LOG_FILE}"
