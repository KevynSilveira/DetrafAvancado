
from __future__ import annotations
from pathlib import Path
from typing import Dict

# Project root: .../src/detraf/env.py -> root is two parents up
ROOT = Path(__file__).resolve().parents[2]
CONFIGS_DIR = ROOT / "configs"
ENV_PATH = CONFIGS_DIR / ".env"

def _parse_env_line(line: str):
    if not line or line.strip().startswith("#") or "=" not in line:
        return None, None
    key, _, val = line.strip().partition("=")
    return key.strip(), val.strip()

def load_env() -> Dict[str, str]:
    d: Dict[str, str] = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text(encoding="utf-8").splitlines():
            k, v = _parse_env_line(line)
            if k:
                d[k] = v
    return d

def save_env(updated: Dict[str, str]) -> None:
    CONFIGS_DIR.mkdir(parents=True, exist_ok=True)
    lines = [f"{k}={v}" for k, v in updated.items()]
    ENV_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")

def get(key: str, default: str | None = None) -> str | None:
    return load_env().get(key, default)

def set_many(pairs: Dict[str, str]) -> None:
    env = load_env()
    env.update({k: str(v) for k, v in pairs.items() if v is not None})
    save_env(env)
