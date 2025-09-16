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
    Verifica existência das tabelas do batimento avançado sem alterar estrutura.
    """
    with conn.cursor() as cur:
        has_arq = _table_exists(cur, "detraf_arquivo_batimento_avancado")
        has_proc = _table_exists(cur, "detraf_processado_batimento_avancado")

    if has_arq and has_proc:
        ok("Schema verificado (tabelas avançadas presentes).")
    elif has_arq and not has_proc:
        warn("Tabela detraf_processado_batimento_avancado não encontrada.")
    elif not has_arq and has_proc:
        warn("Tabela detraf_arquivo_batimento_avancado não encontrada.")
    else:
        warn("Tabelas avançadas não encontradas (arquivo/processado).")

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

    # 3) limpeza solicitada (truncate) — só se existirem (não toca cdr/numeros_portados/cadup)
    try:
        _truncate_if_exists(conn, "detraf_arquivo_batimento_avancado")
        _truncate_if_exists(conn, "detraf_processado_batimento_avancado")
        ok("Preparação concluída (tabelas avançadas limpas). Próxima etapa: importação do DETRAF (layout fixo).")
    finally:
        conn.close()

    return dt_ini.strftime("%Y-%m-%d %H:%M:%S"), dt_fim.strftime("%Y-%m-%d %H:%M:%S")

# === Pipeline: comparação/matching (mínimo seguro) ===
def processar_match() -> None:
    """Executa o batimento real delegando ao módulo ``match_cdr``.

    Esta função mantém a assinatura utilizada pelo ``cli`` e simplesmente
    encaminha a chamada para :func:`match_cdr.processar_match`. Qualquer erro
    é capturado e exibido como aviso para não interromper o fluxo principal.
    """

    try:
        from .match_cdr import processar_match as _match
    except Exception as ex:
        warn(f"Módulo de matching indisponível: {ex}")
        return

    try:
        _match()
    except Exception as ex:
        warn(f"Falha ao executar matching: {ex}")
