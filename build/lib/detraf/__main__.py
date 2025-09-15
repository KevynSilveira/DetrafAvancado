import sys
from pathlib import Path

# Suporte ao modo portátil: tenta adicionar vendor ao sys.path
HERE = Path(__file__).resolve().parent  # .../src/detraf
for cand in ((HERE.parent / "vendor"), (HERE.parents[1] / "vendor")):
    if cand.exists():
        sys.path.insert(0, str(cand.resolve()))
        break

from .cli import app

if __name__ == "__main__":
    sys.exit(app())

