# Project guide

- Read `README.md` and `docs/workflow.md` before changing the tracker.
- The reusable package lives in `src/exptrack`; keep research-specific models,
  datasets, accounts, source artifacts, and training code outside this repository.
- Preserve manifest v1 and existing registry compatibility. Unknown submission or
  scheduler state must never become an automatic retry.
- Keep Slurm state separate from scientific result acceptance. Never certify a
  result from Slurm completion alone, or let attempts overwrite earlier outputs.
- Run `python -m unittest discover -s tests -v` after installing the package.
  Tests use synthetic outputs and fake Slurm commands; they submit no jobs.
- Check a built wheel in an isolated environment outside the source tree after
  packaging changes. Do not claim mocked tests establish real-cluster coverage.
- Do not submit Slurm jobs without session authorization. Use the user's own
  allocation and compute-accessible private outputs.
