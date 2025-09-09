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

from typing import Optional
import re

from .log import ok


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
        SELECT id, data_hora, eot_de_a, eot_de_b,
               REGEXP_REPLACE(assinante_a_numero, '[^0-9]', '') AS a_num,
               REGEXP_REPLACE(assinante_b_numero, '[^0-9]', '') AS b_num
        FROM detraf
        WHERE data_hora BETWEEN %s AND %s
        """,
        (min_dt, max_dt),
    )
    ok(f"Tabela temporária criada: {tmp_name}")


def criar_tmp_cdr(cur, tmp_name: str, min_dt, max_dt) -> None:
    """Cria tabela do CDR com números normalizados.

    A tabela é recriada a cada execução, mantendo os registros normalizados
    para inspeção posterior.
    """

    # Recria tabela persistente
    cur.execute(f"DROP TABLE IF EXISTS {tmp_name}")
    cur.execute(
        f"""
        CREATE TABLE {tmp_name} (
            id BIGINT,
            calldate DATETIME,
            src VARCHAR(32),
            dst VARCHAR(32),
            EOT_A VARCHAR(32),
            EOT_B VARCHAR(32),
            duration INT,
            billsec INT,
            sentido VARCHAR(16),
            disposition VARCHAR(32)
        )
        """
    )

    # Coleta dos registros dentro da janela desejada
    cur.execute(
        """
        SELECT id, calldate, src, dst, EOT_A, EOT_B,
               duration, billsec, sentido, disposition
        FROM cdr
        WHERE calldate BETWEEN %s AND %s
        """,
        (min_dt, max_dt),
    )

    rows = cur.fetchall()
    ins_rows = []
    for row in rows:
        src_norm = _normalizar_numero(row["src"])
        if not src_norm:
            continue
        ddd_src = src_norm[:2]

        dst_norm = _normalizar_numero(row["dst"], ddd_ref=ddd_src, is_dst=True)
        if not dst_norm:
            continue

        ins_rows.append(
            (
                row["id"],
                row["calldate"],
                src_norm,
                dst_norm,
                row["EOT_A"],
                row["EOT_B"],
                row["duration"],
                row["billsec"],
                row["sentido"],
                row["disposition"],
            )
        )

    if ins_rows:
        cur.executemany(
            f"""
            INSERT INTO {tmp_name}
                (id, calldate, src, dst, EOT_A, EOT_B,
                 duration, billsec, sentido, disposition)
            VALUES
                (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            ins_rows,
        )

    ok(f"Tabela criada: {tmp_name}")
