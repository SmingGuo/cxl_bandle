#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
SWEEP_SCRIPT="${SWEEP_SCRIPT:-${ROOT_DIR}/scripts/run_bandle_poisson_vm_sweep.py}"
RUN_SCRIPT="${RUN_SCRIPT:-${ROOT_DIR}/scripts/run_bandle_poisson_vm.sh}"

COMMON_ARGS=(
    --run-script "${RUN_SCRIPT}"
    --max-ops 10000000
    --key-bytes 16
    --read-to-leader 1
    --nodes 3
    --multinode-write 1
    --resume
)

run_sweep() {
    local name="$1"
    local results_dir="$2"
    shift 2

    echo "==== Running ${name} ===="
    "${PYTHON_BIN}" "${SWEEP_SCRIPT}" \
        --results-dir "${results_dir}" \
        "${COMMON_ARGS[@]}" \
        "$@"
}

run_sweep_and_wait() {
    local name="$1"
    shift

    run_sweep "${name}" "$@" &
    local sweep_pid=$!

    echo "==== Waiting for ${name} (pid=${sweep_pid}) ===="
    wait "${sweep_pid}"
    echo "==== Finished ${name} ===="
}

run_sweep_and_wait \
    "bandle sweep" \
    "${ROOT_DIR}/build/_sweep_bandle_new" \
    --value-bytes 64,128,256,512,1024 \
    --read-ratios 0.1,0.25,0.5,0.75,0.9 \
    --qps 10M \
    "$@"

run_sweep_and_wait \
    "bandle qps sweep" \
    "${ROOT_DIR}/build/_sweep_qps_bandle_new" \
    --value-bytes 128 \
    --read-ratios 0.5 \
    --qps 0.5M,1M,2M,3M,4M,5M,7.5M,10M,20M \
    "$@"

echo "Bandle sweep and qps sweeps finished."
