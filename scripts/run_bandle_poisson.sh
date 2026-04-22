#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_DIR="${ROOT_DIR}/scripts"
BUILD_DIR="${ROOT_DIR}/build"

BIN="${BUILD_DIR}/bandle_replay"
BIN_GEN="${BUILD_DIR}/bandle_trace_gen"
BIN_AGG="${BUILD_DIR}/bandle_aggregate"

# Workload parameters (aligned with simple_test run_ycsb_poisson_trace.sh defaults)
MAX_OPS=${MAX_OPS:-10000000}
QPS=${QPS:-1800000}
PRELOAD_COUNT=${PRELOAD_COUNT:-1000}
RECORDCOUNT=${RECORDCOUNT:-1000000}
KEY_BYTES=${KEY_BYTES:-42}
VALUE_BYTES=${VALUE_BYTES:-101}
READ_RATIO=${READ_RATIO:-0.25}
KEY_DIST=${KEY_DIST:-zipfian}
ZIPF_THETA=${ZIPF_THETA:-0.735}
READ_TO_LEADER=${READ_TO_LEADER:-1}
MULTINODE_WRITE=${MULTINODE_WRITE:-1}

# Only generate datasets, do not run replay (useful on machines without a shared CXL mapping).
SKIP_REPLAY=${SKIP_REPLAY:-0}

# Runtime parameters
NODES=${NODES:-3}
# Default to the PCI BAR resource path (used by the multi-VM setup).
# You can still override to /dev/daxX.Y if applicable.
DAX_DEV=${DAX_DEV:-/sys/bus/pci/devices/0000:00:06.0/resource2}
CPU_PER_NODE=${CPU_PER_NODE:-8}
CPU_PIN_MODE=${CPU_PIN_MODE:-disjoint}  # disjoint|shared
CPU_BASE=${CPU_BASE:-32}
POLL_IDLE_US=${POLL_IDLE_US:-0}
NOOP_US=${NOOP_US:-500}
STATS_PREFIX=${STATS_PREFIX:-bandle_stats_node}
BATCH_SIZE=${BATCH_SIZE:-64}
PIPELINE_WORKERS=${PIPELINE_WORKERS:-1}
PERF_STATS=${PERF_STATS:-0}
# Store generated datasets under the bandle workspace by default.
# (Avoid writing into the baseline simple_test directory.)
DATASET_DIR=${DATASET_DIR:-"${SCRIPT_DIR}/../gen_datasets"}
DATASET_NAME_PREFIX=${DATASET_NAME_PREFIX:-""}
START_SIGNAL=${START_SIGNAL:-${SCRIPT_DIR}/start_bandle.flag.$$}
READY_PREFIX=${READY_PREFIX:-${SCRIPT_DIR}/ready_node}
RUN_ID=${RUN_ID:-$$}

# Ensure cleanup trap can always reference PIDS even when we exit early (e.g., SKIP_REPLAY=1).
PIDS=()
cpu_list_for_node() {
    local node_id="$1"
    local per="${CPU_PER_NODE}"
    if [[ "${per}" -le 0 ]]; then
        echo ""
        return 0
    fi
    if [[ "${CPU_PIN_MODE}" == "shared" ]]; then
        local end=$((per - 1))
        echo "0-${end}"
        return 0
    fi
    local start=$((CPU_BASE + (node_id - 1) * per))
    local end=$((start + per - 1))
    echo "${start}-${end}"
}

TOTAL_CPUS=$(nproc --all)

dataset_file_for_node() {
    local node_id="$1"
    echo "${DATASET_BASE_PATH}__node${node_id}_dataset.txt"
}

cleanup() {
    rm -f "${START_SIGNAL}" || true
    rm -f "${READY_PREFIX}"*.flag.$$ || true
    rm -f "${SCRIPT_DIR}/${STATS_PREFIX}"*.csv.$$ || true
    if [[ "${#PIDS[@]}" -gt 0 ]]; then
        for pid in "${PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" >/dev/null 2>&1 || true
                wait "$pid" 2>/dev/null || true
            fi
        done
    fi
}
trap cleanup EXIT INT TERM

mkdir -p "${DATASET_DIR}"

DATASET_KEY="${DATASET_NAME_PREFIX}ycsb__nodes${NODES}__max${MAX_OPS}__qps${QPS}__rtl${READ_TO_LEADER}__mnw${MULTINODE_WRITE}__rc${RECORDCOUNT}__kb${KEY_BYTES}__vb${VALUE_BYTES}__rr${READ_RATIO}__kdist${KEY_DIST}__theta${ZIPF_THETA}__putonly"
DATASET_BASE_PATH="${DATASET_DIR}/${DATASET_KEY}"

echo "================================================"
echo "Bandle YCSB Poisson Replay"
echo "================================================"
echo "Max operations:    ${MAX_OPS}"
echo "Poisson QPS:       ${QPS}"
echo "Preload count:     ${PRELOAD_COUNT}"
echo "CXL device:        ${DAX_DEV}"
echo "Nodes:             ${NODES}"
echo "CPU per node:      ${CPU_PER_NODE} (${CPU_PIN_MODE}, base=${CPU_BASE})"
echo "Pipeline workers:  ${PIPELINE_WORKERS}"
echo "Perf stats:        ${PERF_STATS}"
echo "Read ratio:        ${READ_RATIO}"
echo "Key dist:          ${KEY_DIST} (theta=${ZIPF_THETA})"
echo "Key/value bytes:   ${KEY_BYTES}/${VALUE_BYTES}"
echo "Dataset key:       ${DATASET_KEY}"
echo "Run ID:            ${RUN_ID}"
echo "Skip replay:       ${SKIP_REPLAY}"
echo "================================================"

if [[ "${SKIP_REPLAY}" -eq 0 ]]; then
    if [[ ! -e "${DAX_DEV}" ]]; then
        echo "CXL device ${DAX_DEV} not found"
        exit 1
    fi
fi
if [[ ! -x "${BIN_GEN}" ]]; then
    echo "Generator not found: ${BIN_GEN}. Build first (cmake && make)."
    exit 1
fi

if [[ "${SKIP_REPLAY}" -eq 0 ]]; then
    if [[ "${CPU_PER_NODE}" -gt 0 && ! -x "$(command -v taskset)" ]]; then
        echo "taskset not found; install util-linux or set CPU_PER_NODE=0"
        exit 1
    fi
fi

YCSB_HOME=/home/gsm/dataset/ycsb-0.17.0
if [[ ! -d "${YCSB_HOME}" ]]; then
    echo "YCSB_HOME not found: ${YCSB_HOME}"
    exit 1
fi
if ! command -v java >/dev/null 2>&1 || ! command -v javac >/dev/null 2>&1; then
    echo "java/javac not found on PATH"
    exit 1
fi

BINDING_SRC="${SCRIPT_DIR}/../ycsb_binding/CxlTraceDB.java"
BINDING_BUILD="${SCRIPT_DIR}/cxl_ycsb_binding"
BINDING_CLASSES="${BINDING_BUILD}/classes"
BINDING_JAR="${BINDING_BUILD}/cxl_trace_db.jar"
mkdir -p "${BINDING_CLASSES}"
if [[ ! -f "${BINDING_JAR}" || "${BINDING_SRC}" -nt "${BINDING_JAR}" ]]; then
    rm -rf "${BINDING_CLASSES}"/*
    javac -cp "${YCSB_HOME}/lib/*" -d "${BINDING_CLASSES}" "${BINDING_SRC}"
    jar cf "${BINDING_JAR}" -C "${BINDING_CLASSES}" .
fi

dataset_exists=1
for i in $(seq 1 ${NODES}); do
    f=$(dataset_file_for_node "${i}")
    if [[ ! -f "${f}" ]]; then
        dataset_exists=0
        break
    fi
done

RAW_FILE="${DATASET_BASE_PATH}__raw_leader_dataset.txt"

if [[ "${dataset_exists}" -eq 1 ]]; then
    echo "Found existing datasets; skipping generation."
else
    UPDATE_RATIO=$(python3 - <<PY
r=float("${READ_RATIO}")
print(max(0.0, min(1.0, 1.0 - r)))
PY
)

    MAX_PAYLOAD=$((KEY_BYTES + VALUE_BYTES + 32))

    echo "Generating raw leader dataset via YCSB..."
    java -cp "${BINDING_JAR}:${YCSB_HOME}/lib/*" site.ycsb.Client \
        -t \
        -db com.cxl.consensus.CxlTraceDB \
        -P "${YCSB_HOME}/workloads/workloada" \
        -threads 1 \
        -target 0 \
        -p "recordcount=${RECORDCOUNT}" \
        -p "operationcount=${MAX_OPS}" \
        -p "readproportion=${READ_RATIO}" \
        -p "updateproportion=${UPDATE_RATIO}" \
        -p "insertproportion=0" \
        -p "scanproportion=0" \
        -p "requestdistribution=${KEY_DIST}" \
        -p "zipfianconstant=${ZIPF_THETA}" \
        -p "fieldcount=1" \
        -p "fieldlength=${VALUE_BYTES}" \
        -p "cxl.truncate=true" \
        -p "cxl.preloadcount=${PRELOAD_COUNT}" \
        -p "cxl.qps=${QPS}" \
        -p "cxl.seed=1" \
        -p "cxl.keybytes=${KEY_BYTES}" \
        -p "cxl.valuebytes=${VALUE_BYTES}" \
        -p "cxl.maxpayload=${MAX_PAYLOAD}" \
        -p "cxl.nodes=1" \
        -p "cxl.leaderfile=${RAW_FILE}"

    echo "Splitting dataset: reads balanced; writes multinode=${MULTINODE_WRITE}"
    "${BIN_GEN}" "${RAW_FILE}" "${DATASET_BASE_PATH}__node" "${NODES}" "${MAX_OPS}" "${MULTINODE_WRITE}"

    echo "Removing raw leader dataset: ${RAW_FILE}"
    rm -f "${RAW_FILE}"
fi

for i in $(seq 1 ${NODES}); do
    f=$(dataset_file_for_node "${i}")
    if [[ ! -f "${f}" ]]; then
        echo "Dataset missing after generation: ${f}"
        exit 1
    fi
    lines=$(wc -l < "${f}")
    echo "  Node ${i} dataset: ${lines} lines (${f})"
done

rm -f "${START_SIGNAL}" || true
rm -f "${READY_PREFIX}"*.flag.$$ || true

if [[ "${SKIP_REPLAY}" -eq 1 ]]; then
    echo "SKIP_REPLAY=1 set; datasets generated/verified. Exiting without replay."
    exit 0
fi

PIDS=()

for node_id in $(seq 1 ${NODES}); do
    port=$((5000 + node_id - 1))
    dataset_file=$(dataset_file_for_node "${node_id}")
    ready_file="${READY_PREFIX}${node_id}.flag.$$"
    stats_file="${SCRIPT_DIR}/${STATS_PREFIX}${node_id}.csv.$$"

    CPU_LIST="$(cpu_list_for_node "${node_id}")"
    echo "Starting node ${node_id} (CPU ${CPU_LIST})"
    cmd=("${BIN}" "${DAX_DEV}" "${node_id}" "${port}" \
         --dataset "${dataset_file}" \
         --preload-file "$(dataset_file_for_node 1)" \
         --preload-count "${PRELOAD_COUNT}" \
         --cluster-size "${NODES}" \
            --run-id "${RUN_ID}" \
         --start-signal "${START_SIGNAL}" \
         --ready-file "${ready_file}" \
         --poll-idle-us "${POLL_IDLE_US}" \
         --noop-us "${NOOP_US}" \
         --batch-size "${BATCH_SIZE}" \
            --pipeline-workers "${PIPELINE_WORKERS}" \
            --perf-stats "${PERF_STATS}" \
         --stats-out "${stats_file}")
    if [[ "${CPU_PER_NODE}" -gt 0 ]]; then
        echo "  CPU affinity: ${CPU_LIST}"
        taskset -c "${CPU_LIST}" "${cmd[@]}" &
    else
        "${cmd[@]}" &
    fi
    PIDS+=($!)
    sleep 1
done

echo "Waiting for all nodes to report ready..."
for node_id in $(seq 1 ${NODES}); do
    ready_file="${READY_PREFIX}${node_id}.flag.$$"
    while [[ ! -f "${ready_file}" ]]; do
        sleep 0.1
    done
    echo "  node ${node_id} ready"
done

echo "Sending start signal"
touch "${START_SIGNAL}"

echo "Waiting for nodes to finish..."
for pid in "${PIDS[@]}"; do
    wait "${pid}" || true
done

echo "Bandle replay complete"

if [[ -x "${BIN_AGG}" ]]; then
    echo "Aggregating stats..."
    stats_args=()
    for node_id in $(seq 1 ${NODES}); do
        stats_args+=("${SCRIPT_DIR}/${STATS_PREFIX}${node_id}.csv.$$")
    done
    "${BIN_AGG}" "${stats_args[@]}" || true
else
    echo "Aggregator not found: ${BIN_AGG} (skipping)"
fi
