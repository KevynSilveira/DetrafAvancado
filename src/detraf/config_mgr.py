# -*- coding: utf-8 -*-
from __future__ import annotations
import os
import yaml

CFG_PATH = "configs/app.yaml"

def ensure_config_file():
    os.makedirs(os.path.dirname(CFG_PATH), exist_ok=True)
    if not os.path.exists(CFG_PATH):
        with open(CFG_PATH, "w", encoding="utf-8") as fh:
            yaml.safe_dump({"periodo": None, "eot": None, "arquivo": None}, fh)

def load_config() -> dict:
    ensure_config_file()
    with open(CFG_PATH, "r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}

def save_config(cfg: dict):
    with open(CFG_PATH, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh, sort_keys=False, allow_unicode=True)

def _ask(prompt: str, default: str | None = None):
    v = input(f"{prompt} " + (f"[{default}]: " if default else ": "))
    return (v or default or "").strip()

def prompt_config_if_needed(cfg: dict, force_prompt=False) -> dict:
    cfg = dict(cfg or {})
    if force_prompt or not all(cfg.get(k) for k in ("periodo","eot","arquivo")):
        print("Informe as variáveis do processamento:")
        cfg["periodo"] = _ask("Período de referência (YYYYMM)", cfg.get("periodo"))
        cfg["eot"] = _ask("EOT da análise (3 dígitos)", cfg.get("eot"))
        cfg["arquivo"] = _ask("Caminho do arquivo DETRAF", cfg.get("arquivo") or "data/Detraf.txt")
    return cfg
