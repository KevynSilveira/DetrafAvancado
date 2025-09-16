from __future__ import annotations
"""Funções de normalização de números para CDR e DETRAF."""

from typing import Optional, Tuple, Optional as Opt
import re

from .log import ok

# === EOT helpers: numeros_portados / cadup ===
def _lookup_eot_numeros_portados(cur, numero: str) -> Tuple[Opt[str], Opt[object]]:
    n = _digits(numero)
    if len(n) in (12, 13):
        n = n[2:]
    if len(n) > 11 and n.startswith('55'):
        n = n[2:]
    try:
        cur.execute(
            """
            SELECT eot, data_janela
            FROM numeros_portados
            WHERE numero = %s
            ORDER BY data_janela DESC
            LIMIT 1
            """,
            (n,),
        )
        row = cur.fetchone()
        if not row:
            return None, None
        eot = row["eot"] if isinstance(row, dict) else row[0]
        dj = row["data_janela"] if isinstance(row, dict) else row[1]
        return (str(eot) if eot is not None else None, dj)
    except Exception:
        return None, None


def _national_number(numero: str) -> str:
    n = _digits(numero)
    if len(n) in (12, 13):
        n = n[2:]
    if len(n) > 11 and n.startswith('55'):
        n = n[2:]
    return n


def _split_number_for_cadup(numero: str) -> Tuple[Opt[str], Opt[str], Opt[str], Opt[str]]:
    n = _national_number(numero)
    if len(n) == 11 and n[2] == '9':
        return 'movel', n[:2], n[2:7], n[7:]
    if len(n) == 10 and n[2] in '2345':
        return 'fixo', n[:2], n[2:6], n[6:]
    return None, None, None, None


def _lookup_eot_cadup(cur, numero: str) -> Tuple[Opt[str], str]:
    tipo, cn, prefixo, mcdu = _split_number_for_cadup(numero)
    if not tipo:
        return None, ''
    try:
        cur.execute(
            """
            SELECT empresa_receptora
            FROM cadup
            WHERE CN = %s AND prefixo = %s AND MCDU_inicial <= %s AND MCDU_final >= %s
            LIMIT 1
            """,
            (cn, prefixo, mcdu, mcdu),
        )
        row = cur.fetchone()
        if not row:
            return None, ''
        eot = row["empresa_receptora"] if isinstance(row, dict) else row[0]
        return (str(eot) if eot is not None else None, 'cadup')
    except Exception:
        return None, ''


def _resolve_eot(cur, numero: str, when_dt) -> Tuple[Opt[str], Opt[str], Opt[object]]:
    eot_np, data_jan = _lookup_eot_numeros_portados(cur, numero)
    if eot_np:
        return eot_np, 'numeros_portados', data_jan
    eot_cad, origem = _lookup_eot_cadup(cur, numero)
    if eot_cad:
        return eot_cad, origem, None
    return None, None, None


def _digits(valor: Optional[str]) -> str:
    if not valor:
        return ""
    return re.sub(r"[^0-9]", "", valor)


def _normalizar_numero(numero: str, ddd_ref: Optional[str] = None, *, is_dst: bool = False) -> Optional[str]:
    digitos = _digits(numero)
    if not digitos:
        return None

    if digitos.startswith("55") and len(digitos) > 11:
        digitos = digitos[2:]

    # 0800 (tarifação reversa) — seu cenário usa "800", mas deixo a regra
    if is_dst and len(digitos) >= 10:
        sufixo = digitos[-10:]
        if sufixo.startswith("0800"):
            return "800" + sufixo[4:]
        if sufixo.startswith("800"):
            return sufixo

    if len(digitos) == 9 and digitos[0] == "9":
        return (ddd_ref or "") + digitos if ddd_ref else None

    if len(digitos) == 8 and digitos[0] in "2345":
        return (ddd_ref or "") + digitos if ddd_ref else None

    if len(digitos) >= 11 and digitos[2] == "9":
        return digitos[:11]

    if len(digitos) >= 10 and digitos[2] in "2345":
        return digitos[-10:]

    return None


def criar_tmp_detraf(cur, tmp_name: str, min_dt, max_dt) -> None:
    cur.execute(
        f"""
        CREATE TEMPORARY TABLE {tmp_name} AS
        SELECT id,
               data_hora,
               eot_de_a,
               eot_de_b,
               (
                 CASE
                   WHEN LENGTH(REGEXP_REPLACE(assinante_a_numero, '[^0-9]', '')) IN (12,13)
                        THEN SUBSTRING(REGEXP_REPLACE(assinante_a_numero, '[^0-9]', ''), 3)
                   ELSE REGEXP_REPLACE(assinante_a_numero, '[^0-9]', '')
                 END
               ) AS a_num,
               (
                 CASE
                   WHEN LENGTH(REGEXP_REPLACE(assinante_b_numero, '[^0-9]', '')) IN (12,13)
                        THEN SUBSTRING(REGEXP_REPLACE(assinante_b_numero, '[^0-9]', ''), 3)
                   ELSE REGEXP_REPLACE(assinante_b_numero, '[^0-9]', '')
                 END
               ) AS b_num
        FROM detraf_arquivo_batimento_avancado
        WHERE data_hora BETWEEN %s AND %s
        """,
        (min_dt, max_dt),
    )
    ok(f"Tabela temporária criada: {tmp_name}")


def criar_tmp_cdr(cur, tmp_name: str, tmp_detraf_name: str, min_dt, max_dt) -> None:
    """
    Seleciona CDRs candidatos via JOIN com a temporária do DETRAF,
    aplicando a mesma normalização e restringindo a **±5 minutos (300s)**
    em relação a `data_hora` do DETRAF.
    """

    cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_name}")
    cur.execute(
        f"""
        CREATE TEMPORARY TABLE {tmp_name} AS
        SELECT c.id,
               c.calldate,
               (
                 CASE
                   WHEN LENGTH(REGEXP_REPLACE(c.src, '[^0-9]', '')) IN (12,13)
                        THEN SUBSTRING(REGEXP_REPLACE(c.src, '[^0-9]', ''), 3)
                   ELSE REGEXP_REPLACE(c.src, '[^0-9]', '')
                 END
               ) AS src,
               (
                 CASE
                   WHEN LENGTH(REGEXP_REPLACE(c.dst, '[^0-9]', '')) IN (12,13)
                        THEN SUBSTRING(REGEXP_REPLACE(c.dst, '[^0-9]', ''), 3)
                   ELSE REGEXP_REPLACE(c.dst, '[^0-9]', '')
                 END
               ) AS dst,
               c.EOT_A,
               c.EOT_B,
               c.duration,
               c.billsec,
               c.sentido,
               c.disposition
        FROM cdr c
        JOIN {tmp_detraf_name} d
          ON (
               (
                 CASE
                   WHEN LENGTH(REGEXP_REPLACE(c.src, '[^0-9]', '')) IN (12,13)
                        THEN SUBSTRING(REGEXP_REPLACE(c.src, '[^0-9]', ''), 3)
                   ELSE REGEXP_REPLACE(c.src, '[^0-9]', '')
                 END
               ) = d.a_num
              AND (
                 CASE
                   WHEN LENGTH(REGEXP_REPLACE(c.dst, '[^0-9]', '')) IN (12,13)
                        THEN SUBSTRING(REGEXP_REPLACE(c.dst, '[^0-9]', ''), 3)
                   ELSE REGEXP_REPLACE(c.dst, '[^0-9]', '')
                 END
               ) = d.b_num
              AND ABS(TIMESTAMPDIFF(SECOND, d.data_hora, c.calldate)) <= 300
          )
        """
    )
    ok(f"Tabela criada: {tmp_name}")
