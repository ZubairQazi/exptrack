# exptrack

Track independent research experiments using native Slurm arrays: frozen
manifests, separate attempts, validated outputs, and selective retries.

`exptrack` runs as a CLI or Python module. It needs Python 3.10+ on Linux, shared
storage, and the normal Slurm client commands. It has **no runtime Python
dependencies, database, server, or background service**.

## Install

```bash
git clone https://github.com/ZubairQazi/exptrack.git
cd exptrack
python -m pip install .
exptrack --help
```

The distribution is named `exptrack-slurm`; the module and CLI are `exptrack`.
It is not published on PyPI. An installation must be accessible on compute nodes
at the same path. Activate the intended environment before submitting jobs.

## Quick start

The example generates a manifest for three tiny CPU tasks; replace its resource
settings with your own account and partition:

```bash
python examples/make_manifest.py --account YOUR_ACCOUNT --partition YOUR_PARTITION \
  --output /shared/private/demo.json
exptrack init /shared/private/demo.json /shared/private/demo
exptrack submit /shared/private/demo                 # JSON preview only
exptrack submit /shared/private/demo --execute       # actual sbatch
exptrack status /shared/private/demo
exptrack retry /shared/private/demo                 # eligible failures only
```

`python -m exptrack` is equivalent. Commands emit JSON for humans and agents.
Training commands use argv lists and receive `EXPTRACK_OUTPUT_DIR`; write results
there. Specify row counts, unique keys, expected identities, finite metrics,
checksums, and optional scientific validators in the manifest. Slurm completion
alone does not establish experiment completion.

## What it handles

- Frozen task identity and input fingerprints.
- Native Slurm arrays, resource requests, and optional dependencies.
- Separate outputs and runtime evidence for each attempt.
- JSON/CSV/file acceptance checks and optional external validators.
- Conservative accounting reconciliation, selective retry, and manual resolution
  of ambiguous submissions without accidental duplicate jobs.
- Read-only auditing of existing output directories.

It does not train models, choose experimental designs, aggregate scientific
metrics, resume checkpoints, renew allocations, or administer Slurm. Existing
research adapters remain in their own projects. Registry locking assumes one
submission host and a shared filesystem supporting advisory locks and atomic
rename; this is a single-researcher tool, not a multi-user service.

[Workflow reference](docs/workflow.md) describes manifest fields, acceptance
states, recovery, and the agent workflow. Large hash audits belong in a compute
allocation. Unknown/active jobs are not automatically retried.

## Development

```bash
python -m pip install -e .
python -m unittest discover -s tests -v
python -m pip wheel --no-deps . --wheel-dir dist
```

The tests exercise temporary artifacts and fake Slurm responses. A small live
Slurm CPU pilot also verified successful execution, intentional command/output
failures, cancellation, and selective retries. That pilot is not a guarantee of
compatibility with every site's configuration. Federated clusters are unsupported.
