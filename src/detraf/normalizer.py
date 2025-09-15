from __future__ import annotations
"""Funções de normalização de números para CDR e DETRAF.

Este módulo continha apenas uma limpeza simples dos números do CDR. As
novas regras de negócio exigem uma normalização mais rigorosa para que o
batimento com o DETRAF seja confiável. O CDR possui numerações em formatos
diversos (com DDI/DDD, prefixos, etc.) e, em alguns casos, números locais
sem DDD. Também é necessário tratar chamadas para 0800 que possuem
tarifação reversa.

Para facilitar o entendimento e reutilização, a normalização é feita em
Python e os números inválidos são descartados. A tabela gerada contém os
campos relevantes do CDR já normalizados.
"""

from typing import Optional, Tuple, Optional as Opt
import re

from .log import ok

# === EOT helpers: numeros_portados / cadup ===
def _lookup_eot_numeros_portados(cur, numero: str) -> Tuple[Opt[str], Opt[object]]:
    """Tenta obter (eot, data_janela) em ``numeros_portados`` para ``numero``.

    Retorna (None, None) caso não exista ou em falha.
    """
    # Normaliza o número para o padrão nacional (10/11 dígitos)
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
    """Normaliza para padrão nacional (10/11 dígitos):
    - remove não numéricos
    - se 12/13 dígitos, remove 2 à esquerda
    - se começa com 55 e > 11, remove DDI 55
    """
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
    """Resolve EOT dando prioridade absoluta a ``numeros_portados``.

    Regras:
    - Se existir registro em ``numeros_portados`` para o número: retorna esse EOT
      com origem 'numeros_portados' e a respectiva ``data_janela`` (sem filtrar
      pela data da chamada). A decisão sobre desatualização é feita na camada de
      observação usando a data (quando disponível).
    - Se não existir em ``numeros_portados``: tenta CADUP.
    - Caso nenhum retorne: (None, None, None).
    """
    eot_np, data_jan = _lookup_eot_numeros_portados(cur, numero)
    if eot_np:
        return eot_np, 'numeros_portados', data_jan
    eot_cad, origem = _lookup_eot_cadup(cur, numero)
    if eot_cad:
        return eot_cad, origem, None
    return None, None, None


def _digits(valor: Optional[str]) -> str:
    """Remove caracteres não numéricos de ``valor``."""
    if not valor:
        return ""
    return re.sub(r"[^0-9]", "", valor)


def _normalizar_numero(numero: str, ddd_ref: Optional[str] = None, *, is_dst: bool = False) -> Optional[str]:
    """Normaliza ``numero`` segundo as regras de negócio.

    ``ddd_ref`` é usado para completar números locais (sem DDD) do ``dst``
    utilizando o DDD do ``src``. Quando ``is_dst`` é ``True`` são aplicadas
    as regras de identificação de chamadas para 0800.
    """

    digitos = _digits(numero)
    if not digitos:
        return None

    # Remove DDI 55 quando presente
    if digitos.startswith("55") and len(digitos) > 11:
        digitos = digitos[2:]

    # Tratamento específico para 0800 no destino (tarifação reversa)
    if is_dst and len(digitos) >= 10:
        sufixo = digitos[-10:]
        if sufixo.startswith("0800"):
            return "800" + sufixo[4:]
        if sufixo.startswith("800"):
            return sufixo

    # Casos comuns
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
    """Cria tabela temporária do DETRAF com números normalizados.

    Remove caracteres não numéricos de ``assinante_a_numero`` e
    ``assinante_b_numero`` para facilitar o batimento com o CDR.
    """
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
    """Cria tabela temporária do CDR apenas para linhas candidatas a match.

    A seleção é feita via JOIN com a temporária do DETRAF já normalizada
    (``tmp_detraf_name``), aplicando a mesma normalização simples de
    números no lado do CDR (remove não numéricos; se 12/13 dígitos, corta 2 à esquerda)
    e restringindo a ±5 minutos da data/hora do DETRAF.
    """

    cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_name}")
    cur.execute(
        f"""
        CREATE TEMPORARY TABLE {tmp_name} AS
        SELECT c.id,
               c.calldate,
               /* Normalização simples, compatível com tmp_detraf */
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
              AND ABS(TIMESTAMPDIFF(MINUTE, d.data_hora, c.calldate)) <= 5
          )
        WHERE c.calldate BETWEEN %s AND %s
        """,
        (min_dt, max_dt),
    )
    ok(f"Tabela criada: {tmp_name}")
