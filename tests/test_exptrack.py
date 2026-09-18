"""Synthetic lifecycle tests. No Slurm jobs or research data are used."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from exptrack import core


class TrackerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.root = self.base / "study"
        self.task = {"id": "B3-f0-s0", "identity": {"setup": "B3", "fold": 0, "seed": 0},
                     "cwd": str(self.base), "command": [sys.executable, "-c",
                     "import os,json; from pathlib import Path; p=Path(os.environ['EXPTRACK_OUTPUT_DIR']); (p/'results.json').write_text(json.dumps({'leak_check':'PASS','metric':0.7}))"],
                     "outputs": [{"path": "results.json", "kind": "json", "equals": {"leak_check": "PASS"}, "finite": ["metric"]}]}
        self.manifest = {"version": 1, "provenance": {"source": "synthetic"}, "tasks": [self.task]}
        self.source = self.base / "manifest.json"
        core.write(self.source, self.manifest)
        core.initialize(self.source, self.root)

    def submitted(self):
        response = subprocess.CompletedProcess([], 0, "12345\n", "")
        with patch("exptrack.core.subprocess.run", return_value=response) as run:
            result = core.submit(self.root, execute=True)
            self.assertIn("--no-requeue", run.call_args.args[0])
        return result

    def status(self, state=None):
        with patch("exptrack.core.scheduler_states", return_value=({"12345_0": state} if state else {}, [])):
            return core.inventory(self.root)["tasks"][0]

    def test_preview_and_no_duplicate_submission(self):
        before = (self.root / "registry.json").read_bytes()
        with patch("exptrack.core.subprocess.run") as run:
            self.assertEqual(core.submit(self.root)["tasks"], [self.task["id"]])
            run.assert_not_called()
        self.assertEqual(before, (self.root / "registry.json").read_bytes())
        self.submitted()
        self.assertEqual(core.submit(self.root)["tasks"], [])

    def test_success_evidence_mutation_and_duplicate_worker(self):
        self.submitted()
        self.assertEqual(core.worker(self.root, "s000001", 0), 0)
        self.assertEqual(self.status("COMPLETED")["status"], "COMPLETE")
        self.assertEqual(self.status("RUNNING")["status"], "ACTIVE")
        with self.assertRaises(FileExistsError):
            core.worker(self.root, "s000001", 0)
        path = self.root / "attempts/s000001/B3-f0-s0/outputs/results.json"
        core.write(path, {"leak_check": "PASS", "metric": 0.9})
        self.assertEqual(self.status("COMPLETED")["status"], "INVALID")
        with patch("exptrack.core.scheduler_states", return_value=({}, [])):
            self.assertEqual(core.submit(self.root, retry=True)["tasks"], [])

    def test_ambiguous_submission_blocks_until_resolved(self):
        with patch("exptrack.core.subprocess.run", side_effect=subprocess.TimeoutExpired("sbatch", 60)):
            with self.assertRaisesRegex(ValueError, "unresolved"):
                core.submit(self.root, execute=True)
        self.assertEqual(core.inventory(self.root, True)["tasks"][0]["status"], "SUBMISSION_UNKNOWN")
        self.assertEqual(core.submit(self.root)["tasks"], [])
        core.resolve(self.root, "s000001", job_id="12345", reason="Matched scheduler job name and submission timestamp")
        self.assertEqual(self.status("PENDING")["status"], "ACTIVE")

    def test_terminal_missing_and_selective_retry(self):
        self.submitted()
        self.assertEqual(self.status("COMPLETED")["status"], "MISSING")
        self.assertEqual(self.status()["status"], "UNKNOWN")
        with patch("exptrack.core.scheduler_states", return_value=({"12345_0": "TIMEOUT"}, [])):
            plan = core.submit(self.root, retry=True)
        self.assertEqual(plan["tasks"], [self.task["id"]])
        with patch("exptrack.core.scheduler_states", return_value=({}, ["sacct failed"])):
            with self.assertRaisesRegex(ValueError, "query failed"):
                core.submit(self.root, retry=True)

    def test_manifest_and_input_tamper(self):
        core.write(self.root / "manifest.json", {"version": 2})
        with self.assertRaisesRegex(ValueError, "changed"):
            core.load(self.root)

    def test_bad_manifest(self):
        for modify in [lambda m: m["tasks"].append(copy.deepcopy(m["tasks"][0])),
                       lambda m: m["tasks"][0]["outputs"][0].update(path="../escape"),
                       lambda m: m["tasks"][0].update(environment={"CUDA_VISIBLE_DEVICES": "0"})]:
            m = copy.deepcopy(self.manifest)
            modify(m)
            with self.assertRaises(ValueError):
                core.validate_manifest(m)

    def test_csv_counts_keys_identity_and_finite(self):
        task = {"outputs": [{"path": "rows.csv", "kind": "csv", "rows": 2, "keys": ["seed"],
                            "expected_keys": [[0], [1]], "equals": {"split_check": "PASS"}, "finite": ["metric"]}]}
        path = self.base / "rows.csv"
        for rows in ["0,PASS,0.4\n", "0,PASS,0.4\n0,PASS,0.5\n", "0,PASS,0.4\n2,PASS,0.5\n",
                     "0,PASS,0.4\n1,FAIL,0.5\n", "0,PASS,0.4\n1,PASS,nan\n"]:
            path.write_text("seed,split_check,metric\n" + rows)
            self.assertFalse(core.check_outputs(task, self.base)["valid"])
        path.write_text("seed,split_check,metric\n0,PASS,0.4\n1,PASS,0.5\n")
        self.assertTrue(core.check_outputs(task, self.base)["valid"])

    def test_sacct_array_elements_and_queue_precedence(self):
        responses = [subprocess.CompletedProcess([], 0, "12345|COMPLETED|0:0\n12345_0|COMPLETED|0:0\n12345_1|COMPLETED|1:0\n12345_0.batch|FAILED|1:0\n", ""),
                     subprocess.CompletedProcess([], 0, "12345_0|RUNNING\n", "")]
        with patch("exptrack.core.subprocess.run", side_effect=responses) as run:
            states, warnings = core.scheduler_states(["12345"])
            self.assertIn("--format=JobID%64,State%40,ExitCode", run.call_args_list[0].args[0])
        self.assertEqual(states, {"12345_0": "RUNNING", "12345_1": "FAILED"})
        self.assertEqual(warnings, [])

    def test_worker_failure_and_awaiting_accounting(self):
        self.manifest["tasks"][0]["command"] = [sys.executable, "-c", "raise SystemExit(3)"]
        other = self.base / "failure"
        core.write(self.source, self.manifest)
        core.initialize(self.source, other)
        self.root = other
        self.submitted()
        self.assertEqual(core.worker(self.root, "s000001", 0), 3)
        self.assertEqual(self.status()["status"], "AWAITING_ACCOUNTING")
        self.assertEqual(self.status("FAILED")["status"], "FAILED")

    def test_input_change_and_validator_failure(self):
        for name, extra in [
            ("input", {"inputs": [{"path": str(self.source), "sha256": "0" * 64}]}),
            ("validator", {"validator": [sys.executable, "-c", "raise SystemExit(7)"]}),
        ]:
            manifest = copy.deepcopy(self.manifest)
            manifest["tasks"][0].update(extra)
            core.write(self.source, manifest)
            self.root = self.base / name
            core.initialize(self.source, self.root)
            self.submitted()
            self.assertNotEqual(core.worker(self.root, "s000001", 0), 0)
            self.assertEqual(self.status("FAILED")["status"], "FAILED")

    def test_existing_audit_is_read_only_and_reports_missing(self):
        outputs = self.base / "old-results"
        outputs.mkdir()
        path = outputs / "results.json"
        core.write(path, {"leak_check": "PASS", "metric": 0.6})
        before = path.read_bytes()
        locations = self.base / "locations.json"
        core.write(locations, {self.task["id"]: str(outputs)})
        self.assertEqual(core.audit_existing(self.source, locations)["valid_artifacts"], 1)
        self.assertEqual(before, path.read_bytes())
        core.write(locations, {})
        self.assertEqual(core.audit_existing(self.source, locations)["valid_artifacts"], 0)

    def test_cross_file_checksum(self):
        binary = self.base / "predictions.bin"
        binary.write_bytes(b"scores")
        core.write(self.base / "result.json", {"artifact": {"sha256": core.digest(binary)}})
        task = {"outputs": [{"path": "predictions.bin", "kind": "file",
                            "sha256_from": {"path": "result.json", "field": "artifact.sha256"}}]}
        self.assertTrue(core.check_outputs(task, self.base)["valid"])
        binary.write_bytes(b"modified")
        self.assertFalse(core.check_outputs(task, self.base)["valid"])

    def test_multiple_batches_partial_submission_and_recovery(self):
        second = copy.deepcopy(self.task)
        second.update(id="B3-f0-s1", identity={"setup": "B3", "fold": 0, "seed": 1})
        self.manifest["tasks"].append(second)
        core.write(self.source, self.manifest)
        self.root = self.base / "batches"
        core.initialize(self.source, self.root)
        responses = [subprocess.CompletedProcess([], 0, "12345\n", ""), subprocess.TimeoutExpired("sbatch", 60)]
        with patch("exptrack.core.subprocess.run", side_effect=responses):
            with self.assertRaises(ValueError):
                core.submit(self.root, execute=True, batch_size=1)
        registry = core.load(self.root)[1]
        self.assertEqual(registry["submissions"][0]["job_id"], "12345")
        self.assertIsNone(registry["submissions"][1]["job_id"])
        self.assertEqual(core.submit(self.root)["tasks"], [])
        core.resolve(self.root, "s000002", abandoned=True, reason="Test scheduler confirms no job accepted")
        with patch("exptrack.core.scheduler_states", return_value=({"12345_0": "RUNNING"}, [])):
            self.assertEqual(core.submit(self.root, retry=True)["tasks"], ["B3-f0-s1"])

    def test_cli_generated_wrapper_end_to_end_with_fake_slurm(self):
        bindir = self.base / "bin"
        bindir.mkdir()
        for name, script in {"sbatch": "printf '12345\\n'", "sacct": "printf '12345_0|COMPLETED|0:0\\n'", "squeue": "exit 0"}.items():
            path = bindir / name
            path.write_text("#!/bin/sh\n" + script + "\n")
            path.chmod(0o755)
        env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ["PATH"],
                   SLURM_ARRAY_TASK_ID="0", SLURM_JOB_ID="12345")
        entry = Path(core.__file__).with_name("__main__.py")
        result = subprocess.run([sys.executable, str(entry), "submit", str(self.root), "--execute"],
                                env=env, capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(result.stdout)["batches"][0]["job_id"], "12345")
        script = self.root / "submissions/s000001/worker.sh"
        subprocess.run(["bash", "-n", str(script)], check=True)
        subprocess.run(["bash", str(script)], env=env, check=True)
        result = subprocess.run([sys.executable, str(entry), "status", str(self.root)],
                                env=env, capture_output=True, text=True, check=True)
        self.assertEqual(json.loads(result.stdout)["counts"], {"COMPLETE": 1})



if __name__ == "__main__":
    unittest.main()
