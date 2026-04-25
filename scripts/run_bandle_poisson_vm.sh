#!/bin/bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT_DIR="${ROOT_DIR}/scripts"
BUILD_DIR="${ROOT_DIR}/build"
DATASET_DIR=${DATASET_DIR:-"${ROOT_DIR}/gen_datasets"}

# VM config
VM_USER=${VM_USER:-root}
VM_HOSTS=${VM_HOSTS:-"192.168.100.4 192.168.100.3 192.168.100.5"}
VM_CODE_DIR=${VM_CODE_DIR:-/root/cxl_bandle}
VM_BUILD_DIR=${VM_BUILD_DIR:-/root/cxl_bandle/build}
VM_DATASET_DIR=${VM_DATASET_DIR:-/root/cxl_bandle/gen_datasets}
VM_SSH_OPTS=${VM_SSH_OPTS:-"-o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=5 -o ServerAliveInterval=5 -o ServerAliveCountMax=3"}
SSH_TIMEOUT_SHORT=${SSH_TIMEOUT_SHORT:-8}
LOCK_FILE=${LOCK_FILE:-"${BUILD_DIR}/run_bandle_poisson_vm.lock"}
NODE_EXIT_TIMEOUT_SEC=${NODE_EXIT_TIMEOUT_SEC:-60}

# Workload parameters (aligned with run_bandle_poisson.sh)
MAX_OPS=${MAX_OPS:-10000000}
QPS=${QPS:-10000000}
PRELOAD_COUNT=${PRELOAD_COUNT:-1000}
RECORDCOUNT=${RECORDCOUNT:-1000000}
KEY_BYTES=${KEY_BYTES:-16}
VALUE_BYTES=${VALUE_BYTES:-128}
READ_RATIO=${READ_RATIO:-0.5}
KEY_DIST=${KEY_DIST:-zipfian}
ZIPF_THETA=${ZIPF_THETA:-0.735}
READ_TO_LEADER=${READ_TO_LEADER:-1}
MULTINODE_WRITE=${MULTINODE_WRITE:-1}

# Runtime parameters
NODES=${NODES:-3}
DAX_DEV=${DAX_DEV:-/sys/bus/pci/devices/0000:00:06.0/resource2}
CPU_PER_NODE=${CPU_PER_NODE:-8}
CPU_LIST=${CPU_LIST:-}
POLL_IDLE_US=${POLL_IDLE_US:-0}
NOOP_US=${NOOP_US:-500}
STATS_PREFIX=${STATS_PREFIX:-bandle_stats_node}
BATCH_SIZE=${BATCH_SIZE:-64}
PIPELINE_WORKERS=${PIPELINE_WORKERS:-2}
PERF_STATS=${PERF_STATS:-0}
DATASET_NAME_PREFIX=${DATASET_NAME_PREFIX:-""}
KEEP_STATS=${KEEP_STATS:-0}

# Admission defaults are tuned for the 3-node VM setup with 10M YCSB ops.
# Override with BANDLE_ADMISSION_WINDOW/BANDLE_ADMISSION_BATCH when sweeping.
case "${VALUE_BYTES}" in
    64)
        DEFAULT_BANDLE_ADMISSION_WINDOW=256
        DEFAULT_BANDLE_ADMISSION_BATCH=32
        ;;
    128)
        DEFAULT_BANDLE_ADMISSION_WINDOW=192
        DEFAULT_BANDLE_ADMISSION_BATCH=32
        ;;
    256)
        DEFAULT_BANDLE_ADMISSION_WINDOW=224
        DEFAULT_BANDLE_ADMISSION_BATCH=32
        ;;
    512)
        DEFAULT_BANDLE_ADMISSION_WINDOW=160
        DEFAULT_BANDLE_ADMISSION_BATCH=16
        ;;
    1024)
        DEFAULT_BANDLE_ADMISSION_WINDOW=128
        DEFAULT_BANDLE_ADMISSION_BATCH=16
        ;;
    *)
        DEFAULT_BANDLE_ADMISSION_WINDOW=192
        DEFAULT_BANDLE_ADMISSION_BATCH=32
        ;;
esac
BANDLE_ADMISSION_WINDOW=${BANDLE_ADMISSION_WINDOW:-${DEFAULT_BANDLE_ADMISSION_WINDOW}}
BANDLE_ADMISSION_BATCH=${BANDLE_ADMISSION_BATCH:-${DEFAULT_BANDLE_ADMISSION_BATCH}}

RUN_ID=${RUN_ID:-"$(date +%s)_$$"}
SYNC_DIR_BASE=${SYNC_DIR_BASE:-"/tmp/cxl_bandle_sync_${RUN_ID}"}
ARTIFACT_DIR_BASE=${ARTIFACT_DIR_BASE:-"${VM_BUILD_DIR}/vm_artifacts_${RUN_ID}"}

read -r -a VM_HOSTS_ARR <<< "${VM_HOSTS}"
if [[ "${NODES}" -gt "${#VM_HOSTS_ARR[@]}" ]]; then
    echo "NODES=${NODES} but only ${#VM_HOSTS_ARR[@]} VM hosts provided"
    exit 1
fi
VM_HOSTS_ARR=("${VM_HOSTS_ARR[@]:0:${NODES}}")

mkdir -p "${BUILD_DIR}"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
    echo "Another run_bandle_poisson_vm.sh instance is already running (lock: ${LOCK_FILE})"
    exit 1
fi

DATASET_KEY="${DATASET_NAME_PREFIX}ycsb__nodes${NODES}__max${MAX_OPS}__qps${QPS}__rtl${READ_TO_LEADER}__mnw${MULTINODE_WRITE}__rc${RECORDCOUNT}__kb${KEY_BYTES}__vb${VALUE_BYTES}__rr${READ_RATIO}__kdist${KEY_DIST}__theta${ZIPF_THETA}__putonly"
DATASET_BASE_PATH="${DATASET_DIR}/${DATASET_KEY}"

# For compatibility with the generator/split naming in run_bandle_poisson.sh
DATASET_GEN_PREFIX="${DATASET_BASE_PATH}__node"

dataset_file_for_node() {
    local node_id="$1"
    echo "${DATASET_GEN_PREFIX}${node_id}_dataset.txt"
}

sync_datasets_for_host() {
    local host="$1"
    local node_id="$2"
    local node_dataset
    local preload_dataset
    local files_to_sync=()

    node_dataset="$(dataset_file_for_node "${node_id}")"
    preload_dataset="$(dataset_file_for_node 1)"

    files_to_sync+=("${node_dataset}")
    if [[ "${node_id}" -ne 1 ]]; then
        files_to_sync+=("${preload_dataset}")
    fi

    rsync -az "${files_to_sync[@]}" "${VM_USER}@${host}:${VM_DATASET_DIR}/"
}

ssh_run() {
    local host="$1"
    shift
    ssh -n -T ${VM_SSH_OPTS} "${VM_USER}@${host}" "$@"
}

ssh_run_short() {
    local host="$1"
    shift
    if command -v timeout >/dev/null 2>&1; then
        timeout "${SSH_TIMEOUT_SHORT}" ssh -n -T ${VM_SSH_OPTS} "${VM_USER}@${host}" "$@"
    else
        ssh -n -T ${VM_SSH_OPTS} "${VM_USER}@${host}" "$@"
    fi
}

ssh_run_detached() {
    local host="$1"
    shift
    ssh -f -n -T ${VM_SSH_OPTS} "${VM_USER}@${host}" "$@"
}

ensure_dataset() {
    mkdir -p "${DATASET_DIR}"
    local all_exist=1
    for i in $(seq 1 "${NODES}"); do
        local f
        f="$(dataset_file_for_node "${i}")"
        if [[ ! -f "${f}" ]]; then
            all_exist=0
            break
        fi
    done

    if [[ "${all_exist}" -eq 1 ]]; then
        echo "Found existing datasets; skipping generation."
        return 0
    fi

    echo "Generating datasets locally (no replay)..."
    SKIP_REPLAY=1 \
    MAX_OPS="${MAX_OPS}" \
    QPS="${QPS}" \
    PRELOAD_COUNT="${PRELOAD_COUNT}" \
    RECORDCOUNT="${RECORDCOUNT}" \
    KEY_BYTES="${KEY_BYTES}" \
    VALUE_BYTES="${VALUE_BYTES}" \
    READ_RATIO="${READ_RATIO}" \
    KEY_DIST="${KEY_DIST}" \
    ZIPF_THETA="${ZIPF_THETA}" \
    READ_TO_LEADER="${READ_TO_LEADER}" \
    MULTINODE_WRITE="${MULTINODE_WRITE}" \
    NODES="${NODES}" \
    DATASET_DIR="${DATASET_DIR}" \
    DATASET_NAME_PREFIX="${DATASET_NAME_PREFIX}" \
    "${SCRIPT_DIR}/run_bandle_poisson.sh"
}

RSYNC_EXCLUDES=(--exclude build --exclude .git --exclude gen_datasets)
REMOTE_PIDS=()
REMOTE_EXIT_FILES=()
STATS_LOCAL_DIR=""
RUN_FAILED=0
FETCH_FAILED=0
CLEAN_REMOTE_ARTIFACTS_ON_EXIT=0

cleanup() {
    for idx in "${!VM_HOSTS_ARR[@]}"; do
        local host="${VM_HOSTS_ARR[$idx]}"
        local pid="${REMOTE_PIDS[$idx]:-}"
        if [[ -n "${pid}" ]]; then
            ssh_run_short "${host}" "kill -0 ${pid} >/dev/null 2>&1 && kill ${pid} >/dev/null 2>&1" || true
        fi
        if [[ "${CLEAN_REMOTE_ARTIFACTS_ON_EXIT}" -eq 1 ]]; then
            ssh_run_short "${host}" "rm -rf '${SYNC_DIR_BASE}' '${ARTIFACT_DIR_BASE}'" || true
        else
            ssh_run_short "${host}" "rm -rf '${SYNC_DIR_BASE}'" || true
        fi
    done
    if [[ -n "${STATS_LOCAL_DIR}" && "${KEEP_STATS}" != "1" ]]; then
        rm -rf "${STATS_LOCAL_DIR}" || true
    fi
}
trap cleanup EXIT INT TERM

echo "Cleaning residual processes on VMs..."
for host in "${VM_HOSTS_ARR[@]}"; do
    ssh ${VM_SSH_OPTS} "${VM_USER}@${host}" \
        "pkill -f '[b]andle_replay' >/dev/null 2>&1 || true; \
         pkill -f '[r]un_bandle_poisson_vm_node.sh' >/dev/null 2>&1 || true; \
         rm -rf /tmp/cxl_bandle_sync_* '${ARTIFACT_DIR_BASE}'" || true
done

ensure_dataset

echo "Runtime admission: window=${BANDLE_ADMISSION_WINDOW} batch=${BANDLE_ADMISSION_BATCH} value_bytes=${VALUE_BYTES}"

echo "Syncing code to VMs..."
for idx in "${!VM_HOSTS_ARR[@]}"; do
    node_id=$((idx + 1))
    host="${VM_HOSTS_ARR[$idx]}"
    rsync -az --delete "${RSYNC_EXCLUDES[@]}" "${ROOT_DIR}/" "${VM_USER}@${host}:${VM_CODE_DIR}/"
    ssh_run "${host}" "mkdir -p '${VM_BUILD_DIR}' '${VM_DATASET_DIR}' '${SYNC_DIR_BASE}' '${ARTIFACT_DIR_BASE}'"
    ssh_run "${host}" "cd '${VM_BUILD_DIR}' && cmake .. -DBANDLE_VM=ON && make -j\$(nproc)"
    sync_datasets_for_host "${host}" "${node_id}"
    ssh_run "${host}" "rm -f '${SYNC_DIR_BASE}'/*.flag '${ARTIFACT_DIR_BASE}'/*"
    ssh_run "${host}" "mkdir -p '${SYNC_DIR_BASE}'"
    ssh_run "${host}" "mkdir -p '${ARTIFACT_DIR_BASE}'"
    echo "VM ${host} ready."
done

echo "Launching nodes..."
for idx in "${!VM_HOSTS_ARR[@]}"; do
    node_id=$((idx + 1))
    host="${VM_HOSTS_ARR[$idx]}"
    port=$((5000 + node_id - 1))

    dataset_file="${VM_DATASET_DIR}/$(basename "$(dataset_file_for_node "${node_id}")")"
    leader_dataset="${VM_DATASET_DIR}/$(basename "$(dataset_file_for_node 1)")"

    start_signal="${SYNC_DIR_BASE}/start.flag"
    ready_file="${SYNC_DIR_BASE}/ready_node${node_id}.flag"
    stats_file="${ARTIFACT_DIR_BASE}/${STATS_PREFIX}${node_id}.csv"
    pid_file="${ARTIFACT_DIR_BASE}/node${node_id}.pid"
    log_file="${ARTIFACT_DIR_BASE}/node${node_id}.log"
    exit_file="${ARTIFACT_DIR_BASE}/node${node_id}.exit"

    remote_env="NODE_ID=${node_id} NODES=${NODES} PORT=${port} DAX_DEV='${DAX_DEV}' \
DATASET_FILE='${dataset_file}' PRELOAD_FILE='${leader_dataset}' PRELOAD_COUNT=${PRELOAD_COUNT} RECORDCOUNT=${RECORDCOUNT} \
POLL_IDLE_US=${POLL_IDLE_US} NOOP_US=${NOOP_US} \
BATCH_SIZE=${BATCH_SIZE} PIPELINE_WORKERS=${PIPELINE_WORKERS} BANDLE_ADMISSION_WINDOW=${BANDLE_ADMISSION_WINDOW} BANDLE_ADMISSION_BATCH=${BANDLE_ADMISSION_BATCH} PERF_STATS=${PERF_STATS} RUN_ID=${RUN_ID} \
CPU_PER_NODE=${CPU_PER_NODE} CPU_LIST='${CPU_LIST}' \
START_SIGNAL='${start_signal}' READY_FILE='${ready_file}' STATS_FILE='${stats_file}' PID_FILE='${pid_file}' EXIT_FILE='${exit_file}'"

    launch_cmd="cd '${VM_CODE_DIR}' && setsid nohup env ${remote_env} ./scripts/run_bandle_poisson_vm_node.sh > '${log_file}' 2>&1 < /dev/null &"

    if ! ssh_run_detached "${host}" "bash -lc \"${launch_cmd}\""; then
        echo "Launch failed or timed out on ${host}. Fetching diagnostics..."
        ssh_run_short "${host}" "ls -l '${SYNC_DIR_BASE}' || true"
        ssh_run_short "${host}" "tail -n 80 '${log_file}' || true"
        ssh_run_short "${host}" "ps -ef | grep -E 'bandle_replay|run_bandle_poisson_vm_node' | grep -v grep || true"
        exit 1
    fi

    pid=""
    for _ in $(seq 1 30); do
        pid=$(ssh_run_short "${host}" "cat '${pid_file}' 2>/dev/null || true") || true
        if [[ -n "${pid}" ]]; then
            break
        fi
        sleep 0.1
    done

    REMOTE_PIDS[$idx]="${pid}"
    REMOTE_EXIT_FILES[$idx]="${exit_file}"
    echo "  Node ${node_id} on ${host} started (pid=${pid:-unknown})"
    sleep 1
done

echo "Waiting for all nodes to report ready..."
for idx in "${!VM_HOSTS_ARR[@]}"; do
    node_id=$((idx + 1))
    host="${VM_HOSTS_ARR[$idx]}"
    ready_file="${SYNC_DIR_BASE}/ready_node${node_id}.flag"
    while true; do
        if ssh_run_short "${host}" "test -f '${ready_file}'"; then
            break
        fi
        sleep 0.1
    done
    echo "  node ${node_id} ready"
done

echo "All nodes ready. Sending start signal..."
for host in "${VM_HOSTS_ARR[@]}"; do
    ssh_run_short "${host}" "touch '${SYNC_DIR_BASE}/start.flag'"
done

echo "Waiting for all nodes to exit..."
for idx in "${!VM_HOSTS_ARR[@]}"; do
    host="${VM_HOSTS_ARR[$idx]}"
    pid="${REMOTE_PIDS[$idx]}"
    exit_file="${REMOTE_EXIT_FILES[$idx]:-}"
    node_exit_deadline=$((SECONDS + NODE_EXIT_TIMEOUT_SEC))
    node_exit_forced=0

    while true; do
        if [[ -n "${exit_file}" ]] && ssh_run_short "${host}" "test -f '${exit_file}'"; then
            break
        fi

        if [[ -n "${pid}" ]]; then
            if ! ssh_run_short "${host}" "kill -0 ${pid} >/dev/null 2>&1"; then
                break
            fi
            proc_state=$(ssh_run_short "${host}" "ps -o stat= -p ${pid} 2>/dev/null | head -n 1 | tr -d '[:space:]'" || true)
            if [[ -n "${proc_state}" && "${proc_state:0:1}" == "Z" ]]; then
                break
            fi
        fi

        if (( SECONDS >= node_exit_deadline )); then
            echo "  Node $((idx + 1)) on ${host} did not exit within ${NODE_EXIT_TIMEOUT_SEC}s; forcing shutdown."
            RUN_FAILED=1
            node_exit_forced=1
            if [[ -n "${pid}" ]]; then
                ssh_run_short "${host}" "kill ${pid} >/dev/null 2>&1 || true" || true
                sleep 1
                ssh_run_short "${host}" "kill -0 ${pid} >/dev/null 2>&1 && kill -9 ${pid} >/dev/null 2>&1 || true" || true
                while ssh_run_short "${host}" "kill -0 ${pid} >/dev/null 2>&1"; do
                    sleep 0.2
                done
            fi
            break
        fi

        sleep 0.2
    done

    if [[ -n "${exit_file}" ]]; then
        exit_status=$(ssh_run_short "${host}" "cat '${exit_file}' 2>/dev/null || true" || true)
        if [[ -n "${exit_status}" && "${exit_status}" != "0" ]]; then
            echo "  Node $((idx + 1)) exited with status ${exit_status}"
            ssh_run_short "${host}" "tail -n 120 '${ARTIFACT_DIR_BASE}/node$((idx + 1)).log' || true" || true
            RUN_FAILED=1
        fi
    fi

    if [[ "${node_exit_forced}" -eq 1 ]]; then
        echo "  Node $((idx + 1)) forced to exit"
    else
        echo "  Node $((idx + 1)) exited"
    fi
done

STATS_LOCAL_DIR="${BUILD_DIR}/vm_stats_${RUN_ID}"
mkdir -p "${STATS_LOCAL_DIR}"

LOGS_LATEST_DIR="${BUILD_DIR}/vm_logs_latest"
rm -rf "${LOGS_LATEST_DIR}"
mkdir -p "${LOGS_LATEST_DIR}"

echo "Fetching per-node logs (latest only)..."
for idx in "${!VM_HOSTS_ARR[@]}"; do
    node_id=$((idx + 1))
    host="${VM_HOSTS_ARR[$idx]}"
    remote_log="${ARTIFACT_DIR_BASE}/node${node_id}.log"
    if ! rsync -az "${VM_USER}@${host}:${remote_log}" "${LOGS_LATEST_DIR}/node${node_id}.log"; then
        FETCH_FAILED=1
    fi
done

# Mirror the single-machine launcher output: print the final per-node replay summary lines.
for node_id in $(seq 1 "${NODES}"); do
    log_path="${LOGS_LATEST_DIR}/node${node_id}.log"
    if [[ -f "${log_path}" ]]; then
        # Print the last matching line if present.
        summary_line=$(grep -E "^Replay finished\\." "${log_path}" | tail -n 1 || true)
        if [[ -n "${summary_line}" ]]; then
            echo "${summary_line}"
        fi
    fi
done

echo "Bandle replay complete"

for idx in "${!VM_HOSTS_ARR[@]}"; do
    node_id=$((idx + 1))
    host="${VM_HOSTS_ARR[$idx]}"
    remote_stats="${ARTIFACT_DIR_BASE}/${STATS_PREFIX}${node_id}.csv"
    if ! rsync -az "${VM_USER}@${host}:${remote_stats}" "${STATS_LOCAL_DIR}/"; then
        FETCH_FAILED=1
        RUN_FAILED=1
    fi
done

if [[ -x "${BUILD_DIR}/bandle_aggregate" ]]; then
    echo "Aggregating stats..."
    stats_args=()
    for node_id in $(seq 1 "${NODES}"); do
        stats_args+=("${STATS_LOCAL_DIR}/${STATS_PREFIX}${node_id}.csv")
    done
    if ! "${BUILD_DIR}/bandle_aggregate" "${stats_args[@]}"; then
        RUN_FAILED=1
    fi
fi

if [[ "${RUN_FAILED}" -eq 0 && "${FETCH_FAILED}" -eq 0 ]]; then
    CLEAN_REMOTE_ARTIFACTS_ON_EXIT=1
fi

echo "Done."
if [[ -n "${STATS_LOCAL_DIR}" && "${KEEP_STATS}" == "1" ]]; then
    echo "- Stats dir kept in:  ${STATS_LOCAL_DIR}"
fi
echo "- Latest VM logs kept in:  ${LOGS_LATEST_DIR}"
if [[ "${CLEAN_REMOTE_ARTIFACTS_ON_EXIT}" -eq 0 ]]; then
    echo "- Remote VM artifacts preserved in: ${ARTIFACT_DIR_BASE}"
fi

if [[ "${RUN_FAILED}" -ne 0 ]]; then
    exit 1
fi
