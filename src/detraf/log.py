
from __future__ import annotations
from datetime import datetime
import os
from pathlib import Path

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def section(title: str) -> None:
    bar = "─" * (len(title) + 2)
    print(f"╭{bar}╮\n│ {title} │\n╰{bar}╯")

# Compatibilidade com versões anteriores
header = section

def info(msg: str) -> None:
    print(f"{_ts()} {msg}")

def ok(msg: str) -> None:
    print(f"{_ts()} OK {msg}")

def warn(msg: str) -> None:
    print(f"{_ts()} AVISO {msg}")

def err(msg: str) -> None:
    print(f"{_ts()} ERRO {msg}")


def debug(msg: str) -> None:
    """Registra mensagem de depuração quando DETRAF_DEBUG estiver ativada."""

    if "DETRAF_DEBUG" not in os.environ:
        return

    log_dir = Path("var/logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / "match_debug.log"

    with log_file.open("a", encoding="utf-8") as fp:
        fp.write(f"{_ts()} DEBUG {msg}\n")
