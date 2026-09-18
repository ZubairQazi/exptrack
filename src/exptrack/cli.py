"""All public commands emit JSON; submit/retry preview unless --execute is given."""
import argparse
import json
from pathlib import Path
import sys

from exptrack import core


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="command", required=True)
    q = sub.add_parser("init", help="validate and freeze a manifest in a new tracking directory")
    q.add_argument("manifest", type=Path)
    q.add_argument("root", type=Path)
    q = sub.add_parser("status", help="reconcile Slurm and validate result evidence")
    q.add_argument("root", type=Path)
    q.add_argument("--offline", action="store_true")
    q = sub.add_parser("audit-existing", help="check historical artifacts without changing files or submitting jobs")
    q.add_argument("manifest", type=Path)
    q.add_argument("locations", type=Path)
    for name in ("submit", "retry"):
        q = sub.add_parser(name, help="preview eligible tasks; --execute submits them")
        q.add_argument("root", type=Path)
        q.add_argument("--execute", action="store_true")
        q.add_argument("--task", action="append", dest="selected")
        q.add_argument("--concurrency", type=int, default=3)
        q.add_argument("--batch-size", type=int, default=1000)
        q.add_argument("--dependency")
    q = sub.add_parser("resolve", help="record manual reconciliation of an ambiguous submission")
    q.add_argument("root", type=Path)
    q.add_argument("submission")
    group = q.add_mutually_exclusive_group(required=True)
    group.add_argument("--job-id")
    group.add_argument("--abandoned", action="store_true")
    q.add_argument("--reason", required=True)
    q = sub.add_parser("worker", help="internal Slurm array entry point")
    q.add_argument("root", type=Path)
    q.add_argument("submission")
    q.add_argument("index", type=int)
    a = p.parse_args()
    try:
        if a.command == "init":
            result = core.initialize(a.manifest, a.root)
        elif a.command == "status":
            result = core.inventory(a.root, a.offline)
        elif a.command == "audit-existing":
            result = core.audit_existing(a.manifest, a.locations)
        elif a.command in ("submit", "retry"):
            result = core.submit(a.root, retry=a.command == "retry", execute=a.execute,
                                 selected=a.selected, concurrency=a.concurrency,
                                 batch_size=a.batch_size, dependency=a.dependency)
        elif a.command == "resolve":
            result = core.resolve(a.root, a.submission, a.job_id, a.abandoned, a.reason)
        else:
            return core.worker(a.root, a.submission, a.index)
        print(json.dumps(result, indent=2, allow_nan=False))
        return 0
    except (OSError, ValueError, KeyError, TypeError, StopIteration) as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
