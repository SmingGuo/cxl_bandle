#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import itertools
import json
import os
import re
import shutil
import signal
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple


NUM_RE = r"[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?\d+)?"
CLUSTER_DUR_RE = re.compile(rf"^Duration \(s\):\s*(?P<sec>{NUM_RE})\s*$", re.MULTILINE)
CLUSTER_OK_RE = re.compile(r"^OK ops:\s*(?P<ok>\d+)\s*$", re.MULTILINE)
CLUSTER_READ_RE = re.compile(r"^Read ops:\s*(?P<read>\d+)\s*$", re.MULTILINE)
CLUSTER_WRITE_RE = re.compile(r"^Write ops:\s*(?P<write>\d+)\s*$", re.MULTILINE)
CLUSTER_TPUT_RE = re.compile(rf"^Throughput \(ops/s\):\s*(?P<tput>{NUM_RE})\s*$", re.MULTILINE)
CLUSTER_LAT_RE = re.compile(
    rf"^(?P<kind>Read|Write)\s+p50/p99/mean \((?P<unit>us|cycles)\):\s*"
    rf"(?P<p50>{NUM_RE})\s*/\s*(?P<p99>{NUM_RE})\s*/\s*(?P<mean>{NUM_RE})\s*$",
    re.MULTILINE,
)
AGG_FAILED_RE = re.compile(r"^Aggregate failed:\s*(?P<msg>.*)$", re.MULTILINE)
STATS_DIR_RE = re.compile(r"^- Stats dir kept in:\s*(?P<path>.+?)\s*$", re.MULTILINE)
LOGS_DIR_RE = re.compile(r"^- Latest VM logs kept in:\s*(?P<path>.+?)\s*$", re.MULTILINE)


def parse_qps_token(token: str) -> int:
    m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*([KkMm])?", token.strip())
    if not m:
        raise ValueError(f"invalid qps token: {token!r}")
    value = float(m.group(1))
    suffix = m.group(2)
    if not suffix:
        return int(value)
    return int(value * (1_000 if suffix.lower() == "k" else 1_000_000))


def split_tokens(s: str) -> List[str]:
    return [x.strip() for x in re.split(r"[\s,]+", s.strip()) if x.strip()]


def parse_int_list(s: str) -> List[int]:
    return [int(x) for x in split_tokens(s)]


def parse_float_list(s: str) -> List[float]:
    return [float(x) for x in split_tokens(s)]


def stable_params_key(params: Dict[str, str]) -> str:
    return json.dumps(sorted(params.items()), separators=(",", ":"), ensure_ascii=False)


def load_completed_ok(summary_jsonl: Path) -> "set[str]":
    completed: "set[str]" = set()
    if not summary_jsonl.exists():
        return completed
    with summary_jsonl.open(errors="replace") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except Exception:
                continue
            run = obj.get("run") or {}
            if run.get("status") == "ok" and run.get("params_key"):
                completed.add(str(run["params_key"]))
    return completed


def ensure_header(csv_path: Path, fieldnames: Sequence[str]) -> None:
    if csv_path.exists() and csv_path.stat().st_size > 0:
        return
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="") as f:
        csv.DictWriter(f, fieldnames=list(fieldnames)).writeheader()


def append_csv(csv_path: Path, fieldnames: Sequence[str], row: Dict[str, object]) -> None:
    with csv_path.open("a", newline="") as f:
        csv.DictWriter(f, fieldnames=list(fieldnames)).writerow({k: row.get(k, "") for k in fieldnames})


def append_jsonl(path: Path, row: Dict[str, object]) -> None:
    with path.open("a") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s)


def relative_diff(a: float, b: float) -> float:
    return abs(a - b) / max(abs(a), abs(b), 1e-12)


def parse_metrics(text: str) -> Dict[str, object]:
    out: Dict[str, object] = {
        "duration_s": None,
        "ok_ops": None,
        "read_ops": None,
        "write_ops": None,
        "throughput_ops_per_sec": None,
        "read_unit": None,
        "read_p50": None,
        "read_p99": None,
        "read_mean": None,
        "write_unit": None,
        "write_p50": None,
        "write_p99": None,
        "write_mean": None,
        "aggregate_failed": None,
        "stats_dir": None,
        "logs_dir": None,
    }
    for regex, key in [
        (CLUSTER_DUR_RE, "duration_s"),
        (CLUSTER_OK_RE, "ok_ops"),
        (CLUSTER_READ_RE, "read_ops"),
        (CLUSTER_WRITE_RE, "write_ops"),
        (CLUSTER_TPUT_RE, "throughput_ops_per_sec"),
    ]:
        m = regex.search(text)
        if m:
            out[key] = float(next(iter(m.groupdict().values())))
    for m in CLUSTER_LAT_RE.finditer(text):
        prefix = m.group("kind").lower()
        out[f"{prefix}_unit"] = m.group("unit")
        out[f"{prefix}_p50"] = float(m.group("p50"))
        out[f"{prefix}_p99"] = float(m.group("p99"))
        out[f"{prefix}_mean"] = float(m.group("mean"))
    m = AGG_FAILED_RE.search(text)
    if m:
        out["aggregate_failed"] = m.group("msg").strip() or "1"
    m = STATS_DIR_RE.search(text)
    if m:
        out["stats_dir"] = m.group("path").strip()
    m = LOGS_DIR_RE.search(text)
    if m:
        out["logs_dir"] = m.group("path").strip()
    return out


def move_if_exists(src_str: object, dst: Path) -> str:
    if not src_str:
        return ""
    src = Path(str(src_str))
    if not src.exists():
        return ""
    if dst.exists():
        if dst.is_dir():
            shutil.rmtree(dst)
        else:
            dst.unlink()
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(src), str(dst))
    return str(dst)


class Runner:
    def __init__(self, run_script: Path, dry_run: bool):
        self.run_script = run_script
        self.dry_run = dry_run
        self.current_proc: Optional[subprocess.Popen] = None
        self.stopping = False

    def terminate_current_run(self) -> None:
        proc = self.current_proc
        if not proc or proc.poll() is not None:
            return
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except Exception:
            pass

    def run_one(self, env: Dict[str, str], log_path: Path, timeout_sec: Optional[int]) -> Tuple[int, str]:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        if self.dry_run:
            rendered = " ".join(f"{k}={v}" for k, v in sorted(env.items()) if k in env)
            log_path.write_text(f"DRY_RUN: {rendered} {self.run_script}\n")
            return 0, log_path.read_text()
        with log_path.open("w") as logf:
            self.current_proc = subprocess.Popen(
                [str(self.run_script)],
                stdout=logf,
                stderr=subprocess.STDOUT,
                env=env,
                preexec_fn=os.setsid,
                text=True,
            )
            try:
                rc = self.current_proc.wait(timeout=timeout_sec)
            except subprocess.TimeoutExpired:
                self.terminate_current_run()
                rc = 124
        self.current_proc = None
        return rc, log_path.read_text(errors="replace")


def clean_dataset_cache(root_dir: Path) -> None:
    script = root_dir / "scripts" / "clean_gen_datasets.sh"
    if script.exists():
        subprocess.run(["bash", str(script)], cwd=str(root_dir), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def metric_us(metrics: Dict[str, object], prefix: str) -> Optional[float]:
    if metrics.get(f"{prefix}_unit") == "us" and metrics.get(f"{prefix}_mean") is not None:
        return float(metrics[f"{prefix}_mean"])
    return None


def main() -> int:
    p = argparse.ArgumentParser(description="Sweep run_bandle_poisson_vm.sh parameters and summarize results")
    p.add_argument("--run-script", default=str(Path(__file__).with_name("run_bandle_poisson_vm.sh")))
    p.add_argument("--results-dir", default="")
    p.add_argument("--max-ops", type=int, default=10_000_000)
    p.add_argument("--recordcount", type=int, default=1_000_000)
    p.add_argument("--preload-count", type=int, default=1000)
    p.add_argument("--key-bytes", type=int, default=16)
    p.add_argument("--value-bytes", default="128")
    p.add_argument("--read-ratios", default="0.1,0.25,0.5,0.75,0.9")
    p.add_argument("--qps", default="0.5M,1M,2M,3M,4M,5M,7.5M,10M,20M")
    p.add_argument("--nodes", type=int, default=3)
    p.add_argument("--read-to-leader", type=int, default=1)
    p.add_argument("--multinode-write", type=int, default=1)
    p.add_argument("--multinode-writes", default="")
    p.add_argument("--extra-env", action="append", default=[])
    p.add_argument("--sweep", action="append", default=[])
    p.add_argument("--timeout-sec", type=int, default=900)
    p.add_argument("--min-repeats", type=int, default=2)
    p.add_argument("--max-repeats", type=int, default=8)
    p.add_argument("--stability-threshold-pct", type=float, default=5.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--resume", action="store_true")
    p.add_argument("--no-resume", action="store_true")
    args = p.parse_args()

    if args.multinode_write != 1:
        raise SystemExit("--multinode-write is fixed to 1 for Bandle sweep")
    if args.multinode_writes and any(v != 1 for v in parse_int_list(args.multinode_writes)):
        raise SystemExit("--multinode-writes is fixed to 1 for Bandle sweep")
    if args.min_repeats < 1 or args.max_repeats < args.min_repeats:
        raise SystemExit("invalid repeat settings")

    run_script = Path(args.run_script).resolve()
    if not run_script.exists():
        raise SystemExit(f"run script not found: {run_script}")
    root_dir = Path(__file__).resolve().parents[1]
    results_dir = Path(args.results_dir or root_dir / "build" / "sweep_results_bandle").resolve()
    results_dir.mkdir(parents=True, exist_ok=True)

    fixed_env: Dict[str, str] = {}
    for kv in args.extra_env:
        if "=" not in kv:
            raise SystemExit(f"--extra-env expects KEY=VALUE, got: {kv!r}")
        k, v = kv.split("=", 1)
        fixed_env[k.strip()] = v.strip()

    sweep_dims: List[Tuple[str, List[str]]] = []
    for spec in args.sweep:
        if "=" not in spec:
            raise SystemExit(f"--sweep expects KEY=v1,v2..., got: {spec!r}")
        k, vs = spec.split("=", 1)
        if k.strip() == "MULTINODE_WRITE":
            raise SystemExit("MULTINODE_WRITE is fixed to 1 for Bandle sweep")
        sweep_dims.append((k.strip(), split_tokens(vs)))

    value_bytes_list = parse_int_list(args.value_bytes)
    read_ratios = parse_float_list(args.read_ratios)
    qps_list = [parse_qps_token(x) for x in split_tokens(args.qps)]
    sweep_keys = [k for k, _ in sweep_dims]
    sweep_values = list(itertools.product(*[v for _, v in sweep_dims])) if sweep_dims else [tuple()]
    combos = list(itertools.product(read_ratios, value_bytes_list, qps_list))
    total_runs = len(combos) * len(sweep_values)

    fieldnames = [
        "MULTINODE_WRITE", "effective_max_ops", "read_ratio", "key_bytes", "value_bytes", "qps",
        "seconds", "throughput_ops_per_sec", "read_avg_us", "read_p50_us", "read_p99_us",
        "write_avg_us", "write_p50_us", "write_p99_us", "total", "read", "write", "nodes",
        "read_to_leader", "recordcount", "preload_count", "status", "attempts", "log_path",
        "stats_dir", "logs_dir",
    ]
    summary_csv = results_dir / "summary.csv"
    summary_jsonl = results_dir / "summary.jsonl"
    ensure_header(summary_csv, fieldnames)

    resume_enabled = args.resume and not args.no_resume
    completed_ok = load_completed_ok(summary_jsonl) if resume_enabled else set()
    runner = Runner(run_script, args.dry_run)

    def stop(_signum, _frame):
        runner.stopping = True
        runner.terminate_current_run()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    executed_idx = 0
    combo_idx = 0
    skipped = 0
    for rr, vb, qps in combos:
        for values in sweep_values:
            if runner.stopping:
                return 130
            combo_idx += 1
            sweep_kv = {k: v for k, v in zip(sweep_keys, values)}
            params = {
                "MAX_OPS": str(args.max_ops),
                "RECORDCOUNT": str(args.recordcount),
                "PRELOAD_COUNT": str(args.preload_count),
                "KEY_BYTES": str(args.key_bytes),
                "VALUE_BYTES": str(vb),
                "READ_RATIO": str(rr),
                "QPS": str(qps),
                "NODES": str(args.nodes),
                "READ_TO_LEADER": str(args.read_to_leader),
                "MULTINODE_WRITE": "1",
                **fixed_env,
                **sweep_kv,
            }
            params_key = stable_params_key(params)
            if params_key in completed_ok and not args.dry_run:
                skipped += 1
                print(f"[*] ({combo_idx}/{total_runs}) rr={rr} vb={vb} qps={qps} mnw=1 ... SKIP (already ok)")
                continue

            executed_idx += 1
            label = safe_name(
                f"rr{rr}_k{args.key_bytes}_v{vb}_qps{qps}_ops{args.max_ops}_n{args.nodes}_mnw1_"
                + "_".join(f"{k}{v}" for k, v in sorted(sweep_kv.items()))
            ).replace(".", "p").rstrip("_")
            run_dir = results_dir / "runs" / f"{executed_idx:04d}_{label}"
            run_dir.mkdir(parents=True, exist_ok=True)

            prev_duration: Optional[float] = None
            final_metrics: Dict[str, object] = {}
            final_rc = 1
            final_log = run_dir / "attempt_01.log"
            final_stats_dir = ""
            final_logs_dir = ""
            attempts_run = 0
            consecutive_failures = 0
            status = "fail"
            stabilized = False
            attempt_rows = []

            for attempt in range(1, args.max_repeats + 1):
                attempts_run = attempt
                run_id = f"{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{executed_idx:04d}_a{attempt:02d}"
                log_path = run_dir / f"attempt_{attempt:02d}.log"
                env = os.environ.copy()
                env.update(params)
                env["RUN_ID"] = run_id
                env["KEEP_STATS"] = "1"
                print(f"[*] ({combo_idx}/{total_runs}) {label} try={attempt} ...")
                rc, text = runner.run_one(env, log_path, args.timeout_sec)
                metrics = parse_metrics(text)
                run_failed = rc != 0 or metrics.get("duration_s") is None or metrics.get("aggregate_failed") is not None

                stats_dir = "" if args.dry_run else move_if_exists(metrics.get("stats_dir"), run_dir / f"vm_stats_attempt_{attempt:02d}")
                logs_dir = "" if args.dry_run else move_if_exists(metrics.get("logs_dir"), run_dir / f"vm_logs_attempt_{attempt:02d}")
                duration = float(metrics["duration_s"]) if metrics.get("duration_s") is not None else None
                diff_pct = relative_diff(prev_duration, duration) * 100.0 if prev_duration is not None and duration is not None else None
                attempt_rows.append({"attempt": attempt, "returncode": rc, "duration_s": duration, "duration_diff_pct": diff_pct, "log_path": str(log_path), "stats_dir": stats_dir, "logs_dir": logs_dir})

                final_rc = rc
                final_metrics = metrics
                final_log = log_path
                final_stats_dir = stats_dir
                final_logs_dir = logs_dir

                if run_failed and not args.dry_run:
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        print(f"[!] {label} failed {consecutive_failures} times consecutively; aborting sweep.", file=sys.stderr)
                        break
                    print(f"[!] {label} try={attempt} failed; retrying next try (consecutive failures: {consecutive_failures}/3)", file=sys.stderr)
                    prev_duration = None
                    continue

                consecutive_failures = 0
                if args.dry_run:
                    status = "dry-run"
                    if attempt >= args.min_repeats:
                        stabilized = True
                        break
                elif attempt >= args.min_repeats and prev_duration is not None and diff_pct is not None and diff_pct <= args.stability_threshold_pct:
                    status = "ok"
                    stabilized = True
                    break
                prev_duration = duration

            if not args.dry_run:
                clean_dataset_cache(root_dir)
            if final_rc == 0 and final_metrics.get("throughput_ops_per_sec") is not None and final_metrics.get("duration_s") is not None:
                status = "ok"

            row = {
                "MULTINODE_WRITE": 1,
                "effective_max_ops": final_metrics.get("ok_ops", float(args.max_ops)),
                "read_ratio": rr,
                "key_bytes": args.key_bytes,
                "value_bytes": vb,
                "qps": qps,
                "seconds": final_metrics.get("duration_s"),
                "throughput_ops_per_sec": final_metrics.get("throughput_ops_per_sec"),
                "read_avg_us": metric_us(final_metrics, "read"),
                "read_p50_us": final_metrics.get("read_p50") if final_metrics.get("read_unit") == "us" else None,
                "read_p99_us": final_metrics.get("read_p99") if final_metrics.get("read_unit") == "us" else None,
                "write_avg_us": metric_us(final_metrics, "write"),
                "write_p50_us": final_metrics.get("write_p50") if final_metrics.get("write_unit") == "us" else None,
                "write_p99_us": final_metrics.get("write_p99") if final_metrics.get("write_unit") == "us" else None,
                "total": final_metrics.get("ok_ops"),
                "read": final_metrics.get("read_ops"),
                "write": final_metrics.get("write_ops"),
                "nodes": args.nodes,
                "read_to_leader": args.read_to_leader,
                "recordcount": args.recordcount,
                "preload_count": args.preload_count,
                "status": status,
                "attempts": attempts_run,
                "log_path": str(final_log),
                "stats_dir": final_stats_dir,
                "logs_dir": final_logs_dir,
            }
            if not args.dry_run:
                append_csv(summary_csv, fieldnames, row)
            append_jsonl(summary_jsonl, {"run": {"timestamp": dt.datetime.now().isoformat(timespec="seconds"), "status": status, "returncode": final_rc, "params_key": params_key, "attempts": attempts_run, "stabilized": stabilized, **params}, "summary_row": row, "attempt_rows": attempt_rows, "run_dir": str(run_dir)})
            if status == "fail":
                print(f"[!] Run failed or did not stabilize; see log: {final_log}", file=sys.stderr)
                return 1

    if skipped:
        print(f"[*] Resume: skipped {skipped} already-ok runs")
    print(f"[*] Done. Summary: {summary_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
