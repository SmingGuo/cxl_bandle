#!/bin/bash
set -euo pipefail

NODE_ID=${NODE_ID:-0}
NODES=${NODES:-3}
PORT=${PORT:-0}
DAX_DEV=${DAX_DEV:-/sys/bus/pci/devices/0000:00:06.0/resource2}
DATASET_FILE=${DATASET_FILE:-}
PRELOAD_FILE=${PRELOAD_FILE:-}
PRELOAD_COUNT=${PRELOAD_COUNT:-1000}
RECORDCOUNT=${RECORDCOUNT:-0}
POLL_IDLE_US=${POLL_IDLE_US:-0}
NOOP_US=${NOOP_US:-500}
BATCH_SIZE=${BATCH_SIZE:-64}
PIPELINE_WORKERS=${PIPELINE_WORKERS:-1}
PERF_STATS=${PERF_STATS:-0}
RUN_ID=${RUN_ID:-0}
START_SIGNAL=${START_SIGNAL:-}
READY_FILE=${READY_FILE:-}
STATS_FILE=${STATS_FILE:-}
PID_FILE=${PID_FILE:-}
EXIT_FILE=${EXIT_FILE:-}

CPU_PER_NODE=${CPU_PER_NODE:-0}
CPU_LIST=${CPU_LIST:-}

if [[ "${NODE_ID}" -le 0 || "${PORT}" -le 0 ]]; then
    echo "NODE_ID and PORT must be set"
    exit 1
fi

if [[ -z "${DATASET_FILE}" || -z "${PRELOAD_FILE}" ]]; then
    echo "DATASET_FILE and PRELOAD_FILE must be set"
    exit 1
fi

if [[ ! -e "${DAX_DEV}" ]]; then
    echo "CXL device ${DAX_DEV} not found"
    exit 1
fi

if [[ -n "${PID_FILE}" ]]; then
    echo "$$" > "${PID_FILE}"
fi

BUILD_DIR="$(cd "$(dirname "$0")/../build" && pwd)"

if [[ ! -x "${BUILD_DIR}/bandle_replay" ]]; then
    echo "Error: bandle_replay not found in ${BUILD_DIR}"
    echo "Please build the project first: cd build && cmake .. && make"
    exit 1
fi

TASKSET_PREFIX=()
if [[ "${CPU_PER_NODE}" -gt 0 ]]; then
    if ! command -v taskset >/dev/null 2>&1; then
        echo "Error: taskset not found. Install util-linux or set CPU_PER_NODE=0 to disable pinning."
        exit 1
    fi

    if [[ -n "${CPU_LIST}" ]]; then
        TASKSET_PREFIX=(taskset -c "${CPU_LIST}")
    else
        end=$((CPU_PER_NODE - 1))
        if [[ "${end}" -lt 0 ]]; then
            end=0
        fi
        TASKSET_PREFIX=(taskset -c "0-${end}")
    fi
fi

set +e
"${TASKSET_PREFIX[@]}" "${BUILD_DIR}/bandle_replay" "${DAX_DEV}" "${NODE_ID}" "${PORT}" \
    --dataset "${DATASET_FILE}" \
    --preload-file "${PRELOAD_FILE}" \
    --preload-count "${PRELOAD_COUNT}" \
    --recordcount "${RECORDCOUNT}" \
    --cluster-size "${NODES}" \
    --run-id "${RUN_ID}" \
    --start-signal "${START_SIGNAL}" \
    --ready-file "${READY_FILE}" \
    --poll-idle-us "${POLL_IDLE_US}" \
    --noop-us "${NOOP_US}" \
    --batch-size "${BATCH_SIZE}" \
    --pipeline-workers "${PIPELINE_WORKERS}" \
    --perf-stats "${PERF_STATS}" \
    --stats-out "${STATS_FILE}"
status=$?
set -e

if [[ -n "${EXIT_FILE}" ]]; then
    echo "${status}" > "${EXIT_FILE}"
fi

exit "${status}"
