# Workflow reference

## Manifest and existing validation conventions

A manifest has `version: 1`, a nonempty `provenance` object, an optional `slurm`
resource object, and `tasks`. Each task has:

- `id`: unique stable filename-safe ID.
- `identity`: unique object identifying configuration, graph/features, protocol,
  fold/group, seed, and any other dimensions of the scientific comparison.
- `cwd`: absolute research checkout path; `command`: an argv list, never an
  implicitly evaluated shell string.
- `environment`: optional string mapping. `{output_dir}` and `{attempt_dir}` are
  expanded in argv/environment values. `SLURM_*`, `EXPTRACK_*`, and
  `CUDA_VISIBLE_DEVICES` overrides are rejected.
- `inputs`: optional objects with absolute `path` and expected `sha256`.
- `outputs`: required file/JSON/CSV validation specifications.
- `validator`: optional argv for an existing scientific verifier. It runs in the
  allocation after a successful training command, using the same environment and
  placeholders. A nonzero exit fails the attempt. List every artifact it relies
  on under `outputs`, so subsequent modifications can be detected.

Output paths are relative to the attempt's `outputs/` directory. JSON checks use
dotted field paths. CSV checks require row counts and unique keys; values in CSV
`equals` are strings. For example, the existing six-arm/three-seed LOGO convention
can be represented as:

```json
{
  "path": "group.csv",
  "kind": "csv",
  "rows": 18,
  "keys": ["arm", "seed"],
  "equals": {"held_group_excluded": "PASS", "smoke": "False"},
  "finite": ["wq_mrr", "wq_hits1", "auroc", "ap"]
}
```

Also supply `expected_keys` containing the exact 18 `[arm, seed]` combinations,
and protocol/held-group identity in `equals`. Counts and uniqueness alone cannot
detect a wrong but equally sized set of arms. Wrap the existing NPZ/split checker
as `validator`; the tracker does not infer that scientific contract.

File checks require a nonempty file and compute SHA-256. Optional `sha256` pins
a known digest; `sha256_from: {"path": "results.json", "field":
"prediction_artifact.sha256"}` compares against a JSON result record.

Commands must write into the provided attempt directory. Existing scripts that
hardcode shared canonical output paths need a small adapter before submission.
Do not submit a whole sequential sweep as one task if per-experiment retries are
desired. Keeping a validated group of arms together is also supported: completion
then requires every expected row in that group.

For old results, create a compatible manifest and a JSON mapping from task IDs to
absolute existing output directories:

```bash
python -m exptrack audit-existing study.json locations.json
```

This reads the files without moving, rewriting, or adopting Slurm jobs. Missing
locations remain visible. It reports artifact checks only; custom validators are
explicitly marked as not run. It cannot establish historical command exit status.

## State and retries

| Status | Meaning |
|---|---|
| `UNSUBMITTED` | No submission intent exists. |
| `SUBMISSION_UNKNOWN` | Submission was attempted but no reliable job ID was recorded. |
| `ACTIVE` | Slurm reports a nonterminal array element. |
| `UNKNOWN` | No terminal Slurm state or completed worker record is available. |
| `AWAITING_ACCOUNTING` | Worker failed; allocation termination is not yet confirmed. |
| `COMPLETE` | Worker succeeded and current outputs pass checks and match worker-recorded hashes. |
| `INVALID` | Successful worker's artifacts now fail checks or changed. |
| `MISSING` | Slurm completed but no worker completion record exists. |
| `FAILED` | Terminal allocation or worker failure, or explicitly abandoned submission. |

`COMPLETE` is the manifest's acceptance contract, not a claim of scientifically
valid findings beyond those checks. Slurm `COMPLETED` alone never certifies results.
`status --offline` reads files only; scheduler state is unknown. Status verifies
artifact hashes, so large inventories belong in a CPU allocation. It never reruns
training or custom validators. Scientific aggregation remains in existing reports.

`submit` selects only unsubmitted tasks. `retry` selects failed/missing/invalid
tasks only after Slurm confirms terminal state (or an explicit abandonment).
Scheduler query errors block retries. Unknown states, completed tasks, and active
jobs are never automatically retried. `--task` can narrow either selection; an
ineligible requested task causes an error. Failed attempts remain on disk.

Both commands preview by default and require `--execute` to call `sbatch`.
Authorization for research execution still comes from the user/session; preview
does not imply that an agent should seek repetitive approval for authorized work.

Arrays default to at most 1,000 elements and `%3` concurrency. Set `--batch-size`
for lower site limits and `--concurrency` for desired concurrency **per array**:
multiple batches have independent limits. Optional `--dependency afterok:12345`
supports the existing smoke-before-matrix convention. A dependency failure remains
visible through Slurm; the tracker does not silently remove it. Cancel allocations
with native `scancel`, then inspect status before selecting retries.

## Submission recovery and files

Submission holds an advisory file lock and writes an intent **before** invoking
`sbatch`. If the process dies or Slurm's response is ambiguous, that intent blocks
duplicate submission. After checking the scheduler, attach the actual job:

```bash
python -m exptrack resolve /shared/private/experiment-tracking/study-1 s000001 \
  --job-id 12345 --reason 'Matched array name, timestamp, and command in scontrol'
```

Only if scheduler inspection confirms no job was accepted (or it has been
cancelled and terminated), use `--abandoned --reason 'evidence...'`. Resolution is
an explicit operator assertion, not an automatic timeout heuristic. Never abandon
a possibly active job. The next retry creates new directories.

The directory contains a hash-checked frozen `manifest.json`, an atomically
replaced `registry.json`, per-submission scripts/logs, and per-attempt runtime and
output files. Workers have exclusive start markers; Slurm requeue is disabled so
an attempt cannot overwrite itself. Native `sacct`/`squeue` state is joined on
array **element** IDs, not parent jobs or `.batch` records. Federated clusters are
not supported; an unexpected cluster-qualified response remains unresolved.

Use a filesystem with working advisory locks and atomic rename, and one submission
host per study. Workers do not contend on the registry lock. This is not a
multi-user service or an access-control boundary. The root must be private to the
researcher, on compute-accessible storage. Keep manifests in version control or
publish them deliberately; keep local tracking outputs under the ignored `work/` directory or outside the checkout.

## Agent workflow and verification

1. Inspect status and manifest provenance; check warnings and incomplete counts.
2. Inspect failed-attempt diagnostics and fix the actual cause. Configuration or
   source changes require a new manifest/study, rather than modifying a frozen one.
3. Preview the exact submission/retry subset. Execute within the authorized budget.
4. Give reports only `COMPLETE` task artifacts, retaining expected and missing-run
   counts and matched grouping. Preserve original result files and attempts.

Development checks:

```bash
python -m unittest discover -s tests -p 'test_exptrack.py' -v
```

Tests use temporary artifacts and fake Slurm responses. A small real Slurm CPU array also verified success, command failure, invalid
output, cancellation, and selective retry. No checkpoint/resume, allocation
renewal, automatic retries, metric aggregation, or Slurm administration is provided.
