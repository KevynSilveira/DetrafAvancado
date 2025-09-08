# src/detraf/processing.py
from __future__ import annotations

from datetime import datetime, timedelta
from calendar import monthrange
from typing import Tuple

from .db import get_connection

# === Utilitários de log (isolados para evitar dependência circular com cli.py) ===
def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def info(msg: str) -> None:
    print(f"{_ts()} {msg}")

def ok(msg: str) -> None:
    print(f"{_ts()} OK {msg}")

def warn(msg: str) -> None:
    print(f"{_ts()} AVISO {msg}")

def err(msg: str) -> None:
    print(f"{_ts()} ERRO {msg}")

# === Helpers de data ===
def _first_day_of_month(year: int, month: int) -> datetime:
    return datetime(year, month, 1)

def _last_day_of_month(year: int, month: int) -> datetime:
    last = monthrange(year, month)[1]
    return datetime(year, month, last, 23, 59, 59)

def _add_months(year: int, month: int, delta: int) -> Tuple[int, int]:
    """
    Soma/subtrai meses preservando ano. Ex.: (2025,5,-2) -> (2025,3)
    """
    m = month + delta
    y = year + (m - 1) // 12
    m = (m - 1) % 12 + 1
    return y, m

# === Verificações leves de schema (sem alterar estrutura) ===
def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = %s LIMIT 1",
        (table_name,),
    )
    return cur.fetchone() is not None

def _truncate_if_exists(conn, table_name: str) -> None:
    with conn.cursor() as cur:
        if _table_exists(cur, table_name):
            cur.execute(f"TRUNCATE TABLE `{table_name}`")
            ok(f"Tabela {table_name} truncada para novo processamento.")
        else:
            warn(f"Tabela {table_name} não existe no schema atual. Nada a truncar.")

def _schema_summary(conn) -> None:
    """
    Apenas confirma existência das tabelas sem tentar criar/alterar.
    Fazemos isso para não colidir com o importador, que já assume estrutura específica.
    """
    with conn.cursor() as cur:
        has_detraf = _table_exists(cur, "detraf")
        has_conf = _table_exists(cur, "detraf_conferencia")

    if has_detraf and has_conf:
        ok("Schema verificado (detraf, detraf_conferencia).")
    elif has_detraf and not has_conf:
        warn("Tabela detraf_conferencia não encontrada.")
    elif not has_detraf and has_conf:
        warn("Tabela detraf não encontrada.")
    else:
        warn("Tabelas detraf e detraf_conferencia não encontradas.")

# === Pipeline: preparação ===
def begin_processing(periodo: str, eot: str, arquivo: str) -> Tuple[str, str]:
    """
    - Calcula janela de referência (início = primeiro dia de (periodo - 2 meses), fim = último dia do periodo).
    - Verifica conexão e existência das tabelas.
    - TRUNCATE das tabelas (limpeza) após validação das variáveis (pedido do cliente).
    - Retorna (ini, fim) em string "YYYY-MM-DD HH:MM:SS".
    """
    # período YYYYMM
    try:
        yyyy = int(periodo[:4])
        mm = int(periodo[4:])
    except Exception:
        raise ValueError("Período inválido. Use YYYYMM.")

    # janela: até dois meses anteriores podem aparecer
    y_ini, m_ini = _add_months(yyyy, mm, -2)
    dt_ini = _first_day_of_month(y_ini, m_ini)
    dt_fim = _last_day_of_month(yyyy, mm)

    # Log de contexto (mantém o padrão que você já via)
    print("╭────────────────────────────────────╮")
    print("│  CONTEXTO DE COBRANÇA (OPERADORA)  │")
    print("╰────────────────────────────────────╯")
    info(f"janela_referencia_ini = {dt_ini.strftime('%Y-%m-%d %H:%M:%S')}")
    info(f"janela_referencia_fim = {dt_fim.strftime('%Y-%m-%d %H:%M:%S')}")
    info("Regra: chamadas de até 2 meses anteriores podem aparecer no arquivo deste mês de referência.")

    # Preparação de banco (validação + limpeza)
    print("╭───────────────────────╮")
    print("│  PREPARAÇÃO DE BANCO  │")
    print("╰───────────────────────╯")

    # 1) valida conexão
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            _ = cur.fetchone()
        ok("Conexão verificada.")
    except Exception as ex:
        err(f"Falha ao conectar no banco: {ex}")
        raise

    # 2) verifica se tabelas existem (sem criar/alterar) e mostra resumo
    try:
        _schema_summary(conn)
    except Exception as ex:
        warn(f"Não foi possível verificar o schema: {ex}")

    # 3) limpeza solicitada (truncate) — só se existirem
    try:
        _truncate_if_exists(conn, "detraf")
        _truncate_if_exists(conn, "detraf_conferencia")
        ok("Preparação concluída. Próxima etapa: importação do DETRAF (layout fixo).")
    finally:
        conn.close()

    return dt_ini.strftime("%Y-%m-%d %H:%M:%S"), dt_fim.strftime("%Y-%m-%d %H:%M:%S")

# === Pipeline: comparação/matching (mínimo seguro) ===
def processar_match(periodo: str) -> None:
    """
    Implementação mínima e segura:
      - Se detraf não existir ou estiver vazia, avisa e retorna.
      - Não cria/edita schema. Não escreve em detraf_conferencia para evitar conflito.
    Seu importador/rotina específica pode substituir esta função depois.
    """
    try:
        conn = get_connection()
    except Exception as ex:
        warn(f"Não foi possível conectar para o matching: {ex}")
        return

    try:
        with conn.cursor() as cur:
            if not _table_exists(cur, "detraf"):
                warn("Tabela detraf não existe. Nada a processar.")
                return

            cur.execute("SELECT COUNT(*) AS n FROM detraf")
            row = cur.fetchone() or {"n": 0}
            n = int(row["n"])
            if n == 0:
                warn("Tabela detraf vazia. Nada a processar.")
                return

            # Tentamos detectar se existe coluna data_hora (apenas para mensagem compatível)
            cur.execute("SHOW COLUMNS FROM detraf LIKE 'data_hora'")
            has_dh = cur.fetchone() is not None
            if not has_dh:
                warn("Tabela detraf sem coluna data_hora. Nada a processar.")
                return

            # Aqui entraria a lógica real de matching.
            # Mantemos apenas um log de conclusão para não quebrar o fluxo atual.
            ok("Matching (placeholder) finalizado sem erros.")

    finally:
        conn.close()
