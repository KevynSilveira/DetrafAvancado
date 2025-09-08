
from __future__ import annotations
from datetime import datetime

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def section(title: str) -> None:
    bar = "─" * (len(title) + 2)
    print(f"╭{bar}╮\n│ {title} │\n╰{bar}╯")

def ok(msg: str) -> None:
    print(f"{_ts()} OK {msg}")

def warn(msg: str) -> None:
    print(f"{_ts()} AVISO {msg}")

def err(msg: str) -> None:
    print(f"{_ts()} ERRO {msg}")
