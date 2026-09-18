"""Frozen manifests, attempt evidence, and conservative Slurm reconciliation.

Only the submission host writes the registry, under flock. Workers write separate
attempt directories, avoiding a shared SQLite database on cluster filesystems.
"""
import contextlib
import csv
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
from datetime import datetime, timezone


TERMINAL = {"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY",
            "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "REVOKED"}
RETRYABLE = {"FAILED", "INVALID", "MISSING"}
RESOURCE_KEYS = {"account", "partition", "qos", "gres", "cpus-per-task", "mem", "time", "constraint"}


def now():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def read(path):
    return json.loads(Path(path).read_text())


def write(path, value):
    path = Path(path)
    fd, name = tempfile.mkstemp(dir=path.parent, prefix=".write-")
    try:
        with os.fdopen(fd, "w") as f:
            f.write(json.dumps(value, indent=2, allow_nan=False) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


@contextlib.contextmanager
def locked(root):
    with (Path(root) / "registry.lock").open("a") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        yield


def require(condition, message):
    if not condition:
        raise ValueError(message)


def validate_manifest(m):
    require(m.get("version") == 1, "manifest version must be 1")
    require(isinstance(m.get("provenance"), dict) and m["provenance"], "provenance is required")
    require(isinstance(m.get("tasks"), list) and m["tasks"], "tasks must be nonempty")
    require(set(m.get("slurm", {})) <= RESOURCE_KEYS, "unsupported Slurm resource key")
    require(all(str(v) and "\n" not in str(v) for v in m.get("slurm", {}).values()), "invalid Slurm resource")
    ids, identities = set(), set()
    for task in m["tasks"]:
        key = task.get("id", "")
        require(isinstance(key, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,99}", key), "invalid task id")
        require(key not in ids, f"duplicate task id: {key}")
        ids.add(key)
        require(isinstance(task.get("identity"), dict) and task["identity"], f"{key}: identity is required")
        identity = canonical(task["identity"])
        require(identity not in identities, f"{key}: duplicate experiment identity")
        identities.add(identity)
        require(isinstance(task.get("command"), list) and task["command"] and
                all(isinstance(x, str) and x and "\x00" not in x for x in task["command"]), f"{key}: command must be argv")
        require(Path(task.get("cwd", "")).is_absolute() and Path(task["cwd"]).is_dir(), f"{key}: cwd must be an existing absolute directory")
        env = task.get("environment", {})
        require(isinstance(env, dict) and all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", k) and isinstance(v, str) for k, v in env.items()), f"{key}: invalid environment")
        require(not any(k.startswith(("SLURM_", "EXPTRACK_")) or k == "CUDA_VISIBLE_DEVICES" for k in env), f"{key}: reserved environment variable")
        require(isinstance(task.get("outputs"), list) and task["outputs"], f"{key}: output checks required")
        if "validator" in task:
            require(isinstance(task["validator"], list) and task["validator"] and
                    all(isinstance(v, str) and v for v in task["validator"]), f"{key}: validator must be argv")
        for spec in task["outputs"]:
            require(set(spec) <= {"path", "kind", "rows", "keys", "expected_keys", "equals", "finite", "sha256", "sha256_from"}, f"{key}: unsupported output check")
            path = Path(spec["path"])
            require(not path.is_absolute() and ".." not in path.parts and str(path) != ".", f"{key}: outputs must be attempt-relative")
            require(spec.get("kind") in {"json", "csv", "file"}, f"{key}: unsupported output kind")
            if spec["kind"] == "csv":
                require(isinstance(spec.get("rows"), int) and spec["rows"] > 0 and spec.get("keys"), f"{key}: CSV needs positive rows and unique keys")
        for item in task.get("inputs", []):
            require(Path(item["path"]).is_absolute() and re.fullmatch(r"[0-9a-f]{64}", item["sha256"]), f"{key}: input needs absolute path and sha256")
    canonical(m)


def initialize(manifest, root):
    m = read(manifest)
    validate_manifest(m)
    root = Path(root).resolve()
    root.mkdir(parents=True, exist_ok=False)
    write(root / "manifest.json", m)
    write(root / "registry.json", {"version": 1, "created": now(), "manifest_sha256": digest(root / "manifest.json"), "submissions": []})
    return {"root": str(root), "tasks": len(m["tasks"]), "manifest_sha256": digest(root / "manifest.json")}


def load(root):
    root = Path(root)
    registry = read(root / "registry.json")
    require(digest(root / "manifest.json") == registry["manifest_sha256"], "frozen manifest changed")
    return read(root / "manifest.json"), registry


def field(row, key):
    for part in key.split("."):
        row = row[part]
    return row


def check_outputs(task, directory):
    evidence, errors = [], []
    directory = Path(directory).resolve()
    for spec in task["outputs"]:
        try:
            path = (directory / spec["path"]).resolve()
            require(path.is_relative_to(directory), "output escapes attempt directory")
            require(path.is_file() and path.stat().st_size > 0, "missing or empty output")
            kind = spec["kind"]
            if kind == "json":
                rows = [read(path)]
            elif kind == "csv":
                with path.open(newline="") as f:
                    reader = csv.DictReader(f)
                    require(reader.fieldnames and len(set(reader.fieldnames)) == len(reader.fieldnames), "duplicate or missing CSV columns")
                    rows = list(reader)
                require(len(rows) == spec["rows"], f"row count {len(rows)} != {spec['rows']}")
                require(all(None not in r and None not in r.values() for r in rows), "malformed CSV rows")
                keys = [tuple(r[k] for k in spec["keys"]) for r in rows]
                require(len(set(keys)) == len(keys), "duplicate result keys")
                if "expected_keys" in spec:
                    expected = [tuple(str(v) for v in k) for k in spec["expected_keys"]]
                    require(len(set(expected)) == len(expected) and set(keys) == set(expected), "missing or unexpected result keys")
            else:
                rows = []
            for row in rows:
                for key, value in spec.get("equals", {}).items():
                    require(field(row, key) == value, f"{key}: identity/check mismatch")
                for key in spec.get("finite", []):
                    require(math.isfinite(float(field(row, key))), f"{key}: nonfinite metric")
            sha = digest(path)
            if "sha256" in spec:
                require(sha == spec["sha256"], "checksum mismatch")
            if "sha256_from" in spec:
                source = (directory / spec["sha256_from"]["path"]).resolve()
                require(source.is_relative_to(directory), "checksum source escapes attempt directory")
                require(sha == field(read(source), spec["sha256_from"]["field"]), "recorded artifact checksum mismatch")
            evidence.append({"path": str(path), "sha256": sha, "bytes": path.stat().st_size, "rows": len(rows) if rows else None})
        except (OSError, ValueError, KeyError, TypeError) as e:
            errors.append(f"{spec['path']}: {e}")
    return {"valid": not errors, "errors": errors, "artifacts": evidence}


def scheduler_states(job_ids):
    """Array elements only: parent/step records must never certify a task."""
    states, warnings = {}, []
    ids = sorted(set(job_ids))
    for start in range(0, len(ids), 100):
        batch = ",".join(ids[start:start + 100])
        commands = [
            ["sacct", "-n", "-P", "-j", batch, "--format=JobID%64,State%40,ExitCode"],
        ]
        # Query the user's queue once: --jobs can fail for purged completed jobs.
        if start == 0:
            commands.append(["squeue", "--array", "--noheader", "--user", str(os.getuid()), "--format=%i|%T"])
        for command in commands:
            try:
                p = subprocess.run(command, capture_output=True, text=True, timeout=30, check=True)
                for line in p.stdout.splitlines():
                    parts = [part.strip() for part in line.strip().split("|")]
                    if len(parts) >= 2 and re.fullmatch(r"\d+_\d+", parts[0]):
                        state = parts[1].split()[0].rstrip("+")
                        if command[0] == "sacct" and state == "COMPLETED" and len(parts) > 2 and parts[2] != "0:0":
                            state = "FAILED"
                        if command[0] == "squeue" or parts[0] not in states:
                            states[parts[0]] = state
            except (OSError, subprocess.SubprocessError) as e:
                warnings.append(f"{command[0]} unavailable: {e}")
    return states, warnings


def inventory(root, offline=False):
    root = Path(root).resolve()
    m, registry = load(root)
    states, warnings = ({}, []) if offline else scheduler_states([s["job_id"] for s in registry["submissions"] if s.get("job_id")])
    records = []
    for task in m["tasks"]:
        attempts = [(s, i) for s in registry["submissions"] for i, key in enumerate(s["tasks"]) if key == task["id"]]
        record = {"id": task["id"], "identity": task["identity"], "status": "UNSUBMITTED", "attempts": len(attempts), "scheduler_state": None}
        if attempts:
            s, index = attempts[-1]
            directory = root / "attempts" / s["id"] / task["id"]
            job = f"{s['job_id']}_{index}" if s.get("job_id") else None
            state = states.get(job)
            runtime = read(directory / "runtime.json") if (directory / "runtime.json").exists() else {}
            record.update(submission=s["id"], job_id=job, scheduler_state=state, directory=str(directory), runtime=runtime)
            if s.get("abandoned"):
                record["status"] = "FAILED"
            elif not job:
                record["status"] = "SUBMISSION_UNKNOWN"
            elif state and state not in TERMINAL:
                record["status"] = "ACTIVE"
            elif runtime.get("phase") == "finished" and runtime.get("exit_code") == 0:
                evidence = check_outputs(task, directory / "outputs")
                if evidence["artifacts"] != runtime.get("evidence", {}).get("artifacts"):
                    evidence["valid"] = False
                    evidence["errors"].append("artifacts changed since worker validation")
                record["evidence"] = evidence
                record["status"] = "COMPLETE" if evidence["valid"] else "INVALID"
                if state in TERMINAL and state != "COMPLETED":
                    record["status"] = "FAILED"
            elif state in TERMINAL:
                record["status"] = "MISSING" if state == "COMPLETED" and not runtime else "FAILED"
            elif runtime.get("phase") == "finished":
                # A wrapper can finish before Slurm accounting catches up. A retry
                # is safe only after Slurm confirms the allocation is terminal.
                record["status"] = "AWAITING_ACCOUNTING"
            else:
                record["status"] = "UNKNOWN"
        records.append(record)
    counts = {}
    for r in records:
        counts[r["status"]] = counts.get(r["status"], 0) + 1
    return {"root": str(root), "manifest_sha256": registry["manifest_sha256"], "counts": counts, "warnings": warnings, "tasks": records}


def submit(root, retry=False, execute=False, selected=None, concurrency=3, batch_size=1000, dependency=None):
    root = Path(root).resolve()
    require(concurrency > 0 and 0 < batch_size <= 1000, "positive concurrency and batch size <=1000 required")
    if dependency:
        require(re.fullmatch(r"(?:afterok|afterany):\d+(?::\d+)*", dependency), "dependency must be afterok/afterany with numeric IDs")
    with locked(root):
        m, registry = load(root)
        view = inventory(root, offline=not retry)
        require(not view["warnings"], "scheduler query failed; refusing retry selection")
        abandoned = {s["id"] for s in registry["submissions"] if s.get("abandoned")}
        available = {r["id"] for r in view["tasks"]
                     if r["status"] in (RETRYABLE if retry else {"UNSUBMITTED"})
                     and (not retry or r["scheduler_state"] in TERMINAL or r.get("submission") in abandoned)}
        if selected:
            require(set(selected) <= available, f"tasks not eligible: {sorted(set(selected) - available)}")
            available &= set(selected)
        keys = [t["id"] for t in m["tasks"] if t["id"] in available]
        result = {"execute": execute, "retry": retry, "tasks": keys, "batches": [], "concurrency_per_array": concurrency}
        for offset in range(0, len(keys), batch_size):
            block = keys[offset:offset + batch_size]
            sid = f"s{len(registry['submissions']) + 1:06d}"
            path = root / "submissions" / sid
            command = ["sbatch", "--parsable", "--no-requeue", f"--array=0-{len(block)-1}%{concurrency}", f"--job-name=exptrack-{sid}", f"--output={path}/%A_%a.out", f"--error={path}/%A_%a.err"]
            command += [f"--{k}={v}" for k, v in sorted(m.get("slurm", {}).items())]
            if dependency:
                command.append(f"--dependency={dependency}")
            command.append(str(path / "worker.sh"))
            submission = {"id": sid, "tasks": block, "created": now(), "job_id": None, "command": command}
            result["batches"].append(submission)
            registry["submissions"].append(submission)
            if not execute:
                continue
            path.mkdir(parents=True)
            for key in block:
                (root / "attempts" / sid / key / "outputs").mkdir(parents=True)
            entry = Path(__file__).with_name("__main__.py").resolve()
            invocation = shlex.join([sys.executable, str(entry), "worker", str(root), sid])
            (path / "worker.sh").write_text(f'#!/bin/bash\nset -euo pipefail\nexec {invocation} "${{SLURM_ARRAY_TASK_ID:?}}"\n')
            write(root / "registry.json", registry)  # Durable intent before sbatch.
            try:
                p = subprocess.run(command, capture_output=True, text=True, timeout=60, check=True)
                value = p.stdout.strip()
                require(re.fullmatch(r"\d+(?:;[A-Za-z0-9_.-]+)?", value), "ambiguous sbatch response")
                require(";" not in value, "federated Slurm needs cluster-aware reconciliation; resolve this submission manually")
                submission["job_id"] = value
            except (OSError, ValueError, subprocess.SubprocessError) as e:
                submission["error"] = str(e)
                write(root / "registry.json", registry)
                raise ValueError(f"{sid}: submission unresolved; inspect Slurm and use resolve before retrying: {e}") from e
            write(root / "registry.json", registry)
        return result


def resolve(root, sid, job_id=None, abandoned=False, reason=None):
    require(bool(job_id) != bool(abandoned), "choose a job ID or --abandoned")
    require(reason and reason.strip(), "resolution needs a reason recording scheduler evidence")
    if job_id:
        require(re.fullmatch(r"\d+", job_id), "numeric job ID required")
    with locked(root):
        _, registry = load(root)
        found = [s for s in registry["submissions"] if s["id"] == sid]
        require(len(found) == 1, "unknown submission")
        s = found[0]
        require(not s.get("job_id") and not s.get("abandoned"), "submission already resolved")
        require(not job_id or all(t.get("job_id") != job_id for t in registry["submissions"]), "job already registered")
        s.update(job_id=job_id, abandoned=abandoned, resolution={"at": now(), "reason": reason})
        write(Path(root) / "registry.json", registry)
        return s


def worker(root, sid, index):
    root = Path(root).resolve()
    m, registry = load(root)
    s = next(s for s in registry["submissions"] if s["id"] == sid)
    require(0 <= index < len(s["tasks"]), "array index out of range")
    require(not s.get("abandoned"), "submission abandoned")
    key = s["tasks"][index]
    task = next(t for t in m["tasks"] if t["id"] == key)
    directory = root / "attempts" / sid / key
    # An exclusive marker also prevents Slurm requeue from reusing an attempt.
    with (directory / "started.json").open("x") as f:
        json.dump({"at": now(), "slurm_job_id": os.environ.get("SLURM_JOB_ID")}, f)
    runtime = {"phase": "running", "started": now(), "manifest_sha256": registry["manifest_sha256"], "slurm_job_id": os.environ.get("SLURM_JOB_ID")}
    write(directory / "runtime.json", runtime)
    code = 1
    try:
        for item in task.get("inputs", []):
            require(digest(item["path"]) == item["sha256"], f"input changed: {item['path']}")
        output = str(directory / "outputs")
        expand = lambda x: x.replace("{output_dir}", output).replace("{attempt_dir}", str(directory))
        env = os.environ.copy()
        env.update({k: expand(v) for k, v in task.get("environment", {}).items()})
        env.update(EXPTRACK_OUTPUT_DIR=output, EXPTRACK_ATTEMPT_DIR=str(directory), EXPTRACK_TASK_ID=key)
        command = [expand(v) for v in task["command"]]
        runtime["command"] = command
        write(directory / "runtime.json", runtime)
        code = subprocess.run(command, cwd=task["cwd"], env=env).returncode
        if code == 0 and task.get("validator"):
            validation = [expand(v) for v in task["validator"]]
            code = subprocess.run(validation, cwd=task["cwd"], env=env).returncode
            runtime["validator"] = {"command": validation, "exit_code": code}
        if code == 0:
            evidence = check_outputs(task, output)
            runtime["evidence"] = evidence
            if not evidence["valid"]:
                code = 65
    except (OSError, ValueError) as e:
        runtime["error"] = str(e)
    runtime.update(phase="finished", finished=now(), exit_code=code)
    write(directory / "runtime.json", runtime)
    return code if code >= 0 else 128 - code


def audit_existing(manifest, locations):
    """Read-only checks against old output directories, without adopting jobs."""
    m, paths = read(manifest), read(locations)
    validate_manifest(m)
    require(isinstance(paths, dict), "locations must map task IDs to absolute output directories")
    require(set(paths) <= {t["id"] for t in m["tasks"]}, "locations contain unknown task IDs")
    require(all(isinstance(p, str) and Path(p).is_absolute() for p in paths.values()), "output directories must be absolute")
    records = []
    for task in m["tasks"]:
        evidence = check_outputs(task, paths[task["id"]]) if task["id"] in paths else {"valid": False, "errors": ["no output location"], "artifacts": []}
        records.append({"id": task["id"], "identity": task["identity"], "evidence": evidence,
                        "custom_validator_not_run": bool(task.get("validator"))})
    return {"mode": "read_only_artifact_audit", "expected": len(records),
            "valid_artifacts": sum(r["evidence"]["valid"] for r in records), "tasks": records}
