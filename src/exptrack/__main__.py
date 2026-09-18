"""Support both python -m exptrack and absolute-path Slurm worker scripts."""
from pathlib import Path
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from exptrack.cli import main

if __name__ == "__main__":
    sys.exit(main())
