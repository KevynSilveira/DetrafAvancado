from __future__ import annotations
"""Funções de normalização de números para CDR e DETRAF."""

from .log import ok


def criar_tmp_detraf(cur, tmp_name: str, min_dt, max_dt) -> None:
    """Cria tabela temporária do DETRAF com números normalizados.

    Remove caracteres não numéricos e cria versões curtas (8/9 dígitos)
    para batimento com o CDR. Números iniciados por ``0800`` são
    preservados integralmente.
    """
    cur.execute(
        f"""
        CREATE TEMPORARY TABLE {tmp_name} AS
        SELECT id, data_hora, eot_de_a, eot_de_b,
               assinante_a_numero AS a_num,
               assinante_b_numero AS b_num,
               REGEXP_REPLACE(assinante_a_numero, '[^0-9]', '') AS a_digits,
               REGEXP_REPLACE(assinante_b_numero, '[^0-9]', '') AS b_digits
        FROM detraf
        WHERE data_hora BETWEEN %s AND %s
        """,
        (min_dt, max_dt),
    )
    cur.execute(
        f"""
        ALTER TABLE {tmp_name}
        ADD COLUMN a_short VARCHAR(32),
        ADD COLUMN b_short VARCHAR(32)
        """,
    )
    cur.execute(
        f"""
        UPDATE {tmp_name}
        SET a_short = CASE
                        WHEN a_digits LIKE '0800%' THEN a_digits
                        WHEN CHAR_LENGTH(a_digits)=10 THEN RIGHT(a_digits,8)
                        WHEN CHAR_LENGTH(a_digits)=11 THEN RIGHT(a_digits,9)
                        ELSE a_digits
                      END,
            b_short = CASE
                        WHEN b_digits LIKE '0800%' THEN b_digits
                        WHEN CHAR_LENGTH(b_digits)=10 THEN RIGHT(b_digits,8)
                        WHEN CHAR_LENGTH(b_digits)=11 THEN RIGHT(b_digits,9)
                        ELSE b_digits
                      END
        """,
    )
    ok(f"Tabela temporária criada: {tmp_name}")


def criar_tmp_cdr(cur, tmp_name: str, min_dt, max_dt) -> None:
    """Cria tabela temporária do CDR com números normalizados."""
    cur.execute(
        f"""
        CREATE TEMPORARY TABLE {tmp_name} AS
        SELECT id, calldate, src, dst, EOT_A, EOT_B,
               REGEXP_REPLACE(src, '[^0-9]', '') AS src_digits,
               REGEXP_REPLACE(dst, '[^0-9]', '') AS dst_digits
        FROM cdr
        WHERE calldate BETWEEN %s AND %s
        """,
        (min_dt, max_dt),
    )
    cur.execute(
        f"""
        ALTER TABLE {tmp_name}
        ADD COLUMN src_short VARCHAR(32),
        ADD COLUMN dst_short VARCHAR(32)
        """,
    )
    cur.execute(
        f"""
        UPDATE {tmp_name}
        SET src_short = CASE
                          WHEN CHAR_LENGTH(src_digits)=10 THEN RIGHT(src_digits,8)
                          WHEN CHAR_LENGTH(src_digits)=11 THEN RIGHT(src_digits,9)
                          ELSE src_digits
                        END,
            dst_short = CASE
                          WHEN CHAR_LENGTH(dst_digits)=10 THEN RIGHT(dst_digits,8)
                          WHEN CHAR_LENGTH(dst_digits)=11 THEN RIGHT(dst_digits,9)
                          ELSE dst_digits
                        END
        """,
    )
    ok(f"Tabela temporária criada: {tmp_name}")
