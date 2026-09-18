"""Generate three synthetic CPU tasks without submitting them."""
import argparse
import json
from pathlib import Path
import sys

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--account", required=True)
p.add_argument("--partition", required=True)
p.add_argument("--output", type=Path, required=True)
a = p.parse_args()
tasks = []
for seed in range(3):
    command = [sys.executable, "-c",
               "import json,os,sys; from pathlib import Path; "
               "p=Path(os.environ['EXPTRACK_OUTPUT_DIR']); "
               "(p/'result.json').write_text(json.dumps({'seed':int(sys.argv[1]),'ok':True}))", str(seed)]
    tasks.append({"id": f"demo-s{seed}", "identity": {"experiment": "demo", "seed": seed},
                  "cwd": str(Path.cwd()), "command": command,
                  "outputs": [{"path": "result.json", "kind": "json", "equals": {"seed": seed, "ok": True}}]})
manifest = {"version": 1, "provenance": {"purpose": "synthetic example, no scientific results"},
            "slurm": {"account": a.account, "partition": a.partition, "cpus-per-task": 1,
                      "mem": "512M", "time": "00:02:00"}, "tasks": tasks}
with a.output.open("x") as f:
    json.dump(manifest, f, indent=2)
    f.write("\n")
