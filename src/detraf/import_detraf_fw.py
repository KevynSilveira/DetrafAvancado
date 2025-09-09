from __future__ import annotations
from pathlib import Path
from typing import List, Dict, Any, Tuple
import time
import yaml
import re

# ----------------------------------------------------------------------
# Utilitários de log
# ----------------------------------------------------------------------
from datetime import datetime
def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _ok(msg: str) -> None:
    print(f"{_ts()} OK {msg}")

def _warn(msg: str) -> None:
    print(f"{_ts()} AVISO {msg}")

def _err(msg: str) -> None:
    print(f"{_ts()} ERRO {msg}")

# ----------------------------------------------------------------------
# DB
# ----------------------------------------------------------------------
def _get_conn_cursor():
    """
    Tenta obter conexão com detraf.db.get_conn() ou detraf.db.get_connection().
    Retorna (conn, cur).
    """
    from . import db as dbmod  # import lazy
    conn = None
    if hasattr(dbmod, "get_conn"):
        conn = dbmod.get_conn()
    elif hasattr(dbmod, "get_connection"):
        conn = dbmod.get_connection()
    if conn is None:
        raise RuntimeError("Não foi possível obter conexão (get_conn/get_connection ausentes).")
    cur = conn.cursor()
    return conn, cur

# ----------------------------------------------------------------------
# Layout
# ----------------------------------------------------------------------
def _load_layout(layout_path: str) -> List[Dict[str, Any]]:
    p = Path(layout_path)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Layout não encontrado: {layout_path}")

    with p.open("r", encoding="utf-8") as fh:
        y = yaml.safe_load(fh) or {}
        fields = y.get("fields") or y.get("layout") or []
        norm: List[Dict[str, Any]] = []
        for f in fields:
            # aceita {start (1-based), length} ou {slice_start (0-based), length}
            if "slice_start" in f and "length" in f:
                start = int(f["slice_start"])
                length = int(f["length"])
            else:
                # 'start' vindo 1-based no YAML tradicional -> converter para 0-based
                start_1 = int(f["start"])
                length = int(f["length"])
                start = max(0, start_1 - 1)
            norm.append({"name": f["name"], "slice_start": start, "length": length})
        if not norm:
            raise ValueError("Layout YAML não possui campos válidos (fields/layout).")
        return norm

# ----------------------------------------------------------------------
# Helpers de parse/validação
# ----------------------------------------------------------------------
def _count_lines(path: Path) -> int:
    total = 0
    with path.open("rb") as fh:
        for _ in fh:
            total += 1
    return total

def _slice_fields(line: str, fields: List[Dict[str, Any]]) -> Dict[str, str]:
    rec: Dict[str, str] = {}
    for f in fields:
        s = f["slice_start"]; ln = f["length"]
        rec[f["name"]] = line[s:s+ln]
    return rec

def _clean(s: str) -> str:
    return (s or "").strip()

def _clean_num(s: str) -> str:
    """Remove caracteres não numéricos de ``s`` e faz strip."""
    return re.sub(r"[^0-9]", "", _clean(s))

def _is_valid_date8(s: str) -> bool:
    s = s or ""
    return len(s) == 8 and s.isdigit() and not s.startswith("0000")

def _is_valid_time6(s: str) -> bool:
    s = s or ""
    return len(s) == 6 and s.isdigit() and s[:2] < "24" and s[2:4] < "60" and s[4:6] < "60"

# ----------------------------------------------------------------------
# Progresso
# ----------------------------------------------------------------------
def _progress(curr: int, total: int, started_at: float, every: int = 1000) -> None:
    if curr == 0:
        return
    if (curr % every) != 0 and curr != total:
        return
    elapsed = max(1e-6, time.perf_counter() - started_at)
    rate = curr / elapsed
    remain = max(0.0, (total - curr) / rate) if rate > 0 else 0.0
    width = 40
    filled = int(width * curr / total) if total > 0 else width
    bar = "█" * filled + " " * (width - filled)
    print(f"\rImportando [{bar}] {curr}/{total} lin | {rate:,.1f} lin/s | ETA {remain:,.1f}s", end="")
    if curr == total:
        print("")  # quebra linha final

# ----------------------------------------------------------------------
# INSERT SQL (colunas compatíveis com o schema do projeto)
#  - Usa STR_TO_DATE com %% para escapar o % do Python/PyMySQL
# ----------------------------------------------------------------------
_SQL_INSERT = """
INSERT INTO detraf (
    eot, sequencial, assinante_a_numero, eot_de_a, cnl_de_a, area_local_de_a,
    data_da_chamada, hora_de_atendimento,
    assinante_b_numero, eot_de_b, cnl_de_b, area_local_de_b,
    data_hora
) VALUES (
    %s, %s, %s, %s, %s, %s,
    %s, %s,
    %s, %s, %s, %s,
    STR_TO_DATE(CONCAT(%s, ' ', %s), '%%Y%%m%%d %%H%%i%%s')
)
"""

# ----------------------------------------------------------------------
# Função principal
# ----------------------------------------------------------------------
def importar_fixowidth_para_detraf(caminho: str, layout_path: str, periodo: str | None = None, eot: str | None = None) -> Dict[str, Any]:
    """
    Importa arquivo texto de layout fixo para tabela 'detraf'.
    - caminho: arquivo DETRAF
    - layout_path: YAML com o layout
    - periodo: 'YYYYMM' para filtro (opcional, recomendado)
    - eot: código EOT de contexto a ser gravado na coluna eot (opcional)
    Retorna: dict(total, lidas, inseridos, ignorados_inconsistentes)
    """
    p = Path(caminho)
    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"Arquivo não encontrado: {caminho}")

    fields = _load_layout(layout_path)
    total = _count_lines(p)
    _ok(f"Arquivo encontrado: {p.name} | {total} linhas detectadas")

    conn, cur = _get_conn_cursor()

    # Batch control
    BATCH_SIZE = 1000
    batch: List[Tuple[Any, ...]] = []
    inseridos = 0
    lidas = 0
    ignorados = 0

    t0 = time.perf_counter()
    with p.open("r", encoding="utf-8") as fh:
        for line in fh:
            lidas += 1
            rec = _slice_fields(line.rstrip("\n"), fields)

            # Campos conforme YAML padrão do projeto
            sequencial_raw = _clean_num(rec.get("sequencial", ""))
            try:
                sequencial = int(sequencial_raw) if sequencial_raw else None
            except Exception:
                sequencial = None

            assinante_a_numero = _clean_num(rec.get("assinante_a", ""))
            eot_de_a = _clean(rec.get("eot_de_a", ""))
            cnl_de_a = _clean(rec.get("cnl_de_a", ""))
            area_local_de_a = _clean(rec.get("area_local_de_a", ""))
            data_da_chamada = _clean(rec.get("data_da_chamada", ""))
            hora_de_atendimento = _clean(rec.get("hora_de_atendimento", ""))
            assinante_b_numero = _clean_num(rec.get("assinante_b", ""))
            eot_de_b = _clean(rec.get("eot_de_b", ""))
            cnl_de_b = _clean(rec.get("cnl_de_b", ""))
            area_local_de_b = _clean(rec.get("area_local_de_b", ""))

            # Filtro por periodo YYYYMM (data da chamada)
            if periodo:
                if not _is_valid_date8(data_da_chamada) or data_da_chamada[:6] != periodo:
                    ignorados += 1
                    _progress(lidas, total, t0)  # progresso por linha
                    continue

            # Sanitização de data/hora: STR_TO_DATE lida com NULL/strings inválidas
            data_str = data_da_chamada if _is_valid_date8(data_da_chamada) else None
            hora_str = hora_de_atendimento if _is_valid_time6(hora_de_atendimento) else None

            eot_ctx = (eot or "AUTO").strip() or "AUTO"

            row = (
                eot_ctx, sequencial, assinante_a_numero, eot_de_a, cnl_de_a, area_local_de_a,
                data_da_chamada, hora_de_atendimento,
                assinante_b_numero, eot_de_b, cnl_de_b, area_local_de_b,
                data_str, hora_str,
            )
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                cur.executemany(_SQL_INSERT, batch)
                # Contabiliza pelo que enviamos (independe de rowcount)
                inseridos += len(batch)
                batch.clear()

            _progress(lidas, total, t0)  # progresso por linha

    # Flush final
    if batch:
        cur.executemany(_SQL_INSERT, batch)
        inseridos += len(batch)
        batch.clear()

    # Commit (caso autocommit esteja off)
    try:
        conn.commit()
    except Exception:
        pass

    _progress(total, total, t0)  # garante barra completa

    resumo = {
        "total": int(total),
        "lidas": int(lidas),
        "inseridos": int(inseridos),
        "ignorados_inconsistentes": int(ignorados),
    }
    return resumo
