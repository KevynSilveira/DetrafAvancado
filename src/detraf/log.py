
from __future__ import annotations
from datetime import datetime

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def section(title: str) -> None:
    bar = "─" * (len(title) + 2)
    print(f"╭{bar}╮\n│ {title} │\n╰{bar}╯")

# Compatibilidade com versões anteriores
header = section

def info(msg: str) -> None:
    print(f"{_ts()} {msg}")

def debug(msg: str) -> None:
    print(f"{_ts()} DEBUG {msg}")

def ok(msg: str) -> None:
    print(f"{_ts()} OK {msg}")

def warn(msg: str) -> None:
    print(f"{_ts()} AVISO {msg}")

def err(msg: str) -> None:
    print(f"{_ts()} ERRO {msg}")
