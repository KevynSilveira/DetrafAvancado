# -*- coding: utf-8 -*-
from __future__ import annotations
import sys, time, math

def section(title: str):
    print(f"╭{'─'*35}╮")
    print(f"│  {title.strip():<31}  │")
    print(f"╰{'─'*35}╯")

def ok(msg: str):    print(f"{_ts()} OK {msg}")
def info(msg: str):  print(f"{_ts()} {msg}")
def warn(msg: str):  print(f"{_ts()} AVISO {msg}")
def fail(msg: str):  print(f"{_ts()} ERRO {msg}")
def hrule():         print("─"*100)

def _ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")

class ProgressBar:
    def __init__(self, total: int, prefix: str = "", width: int = 40, unit="it"):
        self.total = max(1, int(total))
        self.prefix = prefix
        self.width = width
        self.unit = unit
        self.start = time.time()
        self.n = 0
        self._last_print = 0

    def update(self, inc: int = 1):
        self.n += inc
        now = time.time()
        # limita prints a ~10 por segundo
        if now - self._last_print < 0.1 and self.n < self.total:
            return
        self._last_print = now
        frac = min(1.0, self.n / self.total)
        filled = int(frac * self.width)
        bar = "█" * filled + "░" * (self.width - filled)
        elapsed = now - self.start
        rate = self.n / elapsed if elapsed > 0 else 0.0
        remain = (self.total - self.n) / rate if rate > 0 else 0.0
        sys.stdout.write(
            f"\r{self.prefix} [{bar}] {self.n}/{self.total} {self.unit} "
            f"| {rate:5.1f} {self.unit}/s | ETA {remain:5.1f}s"
        )
        sys.stdout.flush()
        if self.n >= self.total:
            sys.stdout.write("\n")

    def close(self):
        self.update(0)
