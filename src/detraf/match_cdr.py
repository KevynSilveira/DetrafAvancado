from __future__ import annotations
import time
import pymysql
from .db import get_conn_params, load_env
from .log import header, info, ok, warn, err

def _run_id() -> str:
    return time.strftime("%Y%m%d%H%M%S")

def processar_match(periodo: str) -> None:
    load_env()
    params = get_conn_params()
    runid = _run_id()
    tmp_cdr = f"tmp_cdr_{runid}"
    tmp_detraf = f"tmp_detraf_{runid}"
    tmp_conf = f"tmp_conf_{runid}"

    with pymysql.connect(**params) as conn:
        cur = conn.cursor()

        # Janela do DETRAF importado: min/max data_hora
        cur.execute("SELECT MIN(data_hora) AS min_dt, MAX(data_hora) AS max_dt, COUNT(*) AS total FROM detraf")
        row = cur.fetchone()
        min_dt = row["min_dt"]; max_dt = row["max_dt"]; total_detraf = row["total"]
        if not total_detraf or total_detraf == 0 or not min_dt or not max_dt:
            warn("Tabela detraf vazia ou sem data_hora definido. Nada a processar.")
            return
        info(f"Janela DETRAF detectada: {min_dt} → {max_dt} | {total_detraf} linhas")

        # tmp_detraf com números normalizados curtos
        cur.execute(f"""
        CREATE TEMPORARY TABLE {tmp_detraf} AS
        SELECT id, data_hora, eot_de_a, eot_de_b,
               assinante_a_numero AS a_num,
               assinante_b_numero AS b_num,
               CASE
                 WHEN CHAR_LENGTH(assinante_a_numero)=10 THEN RIGHT(assinante_a_numero,8)
                 WHEN CHAR_LENGTH(assinante_a_numero)=11 THEN RIGHT(assinante_a_numero,9)
                 ELSE assinante_a_numero
               END AS a_short,
               CASE
                 WHEN CHAR_LENGTH(assinante_b_numero)=10 THEN RIGHT(assinante_b_numero,8)
                 WHEN CHAR_LENGTH(assinante_b_numero)=11 THEN RIGHT(assinante_b_numero,9)
                 ELSE assinante_b_numero
               END AS b_short
        FROM detraf
        WHERE data_hora BETWEEN %s AND %s
        """, (min_dt, max_dt))
        ok(f"Tabela temporária criada: {tmp_detraf}")

        # tmp_cdr recortado e com dígitos/short
        cur.execute(f"""
        CREATE TEMPORARY TABLE {tmp_cdr} AS
        SELECT id, calldate, src, dst, EOT_A, EOT_B,
               REGEXP_REPLACE(src, '[^0-9]', '') AS src_digits,
               REGEXP_REPLACE(dst, '[^0-9]', '') AS dst_digits
        FROM cdr
        WHERE calldate BETWEEN %s AND %s
        """, (min_dt, max_dt))
        cur.execute(f"""
        ALTER TABLE {tmp_cdr}
        ADD COLUMN src_short VARCHAR(32), ADD COLUMN dst_short VARCHAR(32)
        """)
        cur.execute(f"""
        UPDATE {tmp_cdr}
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
        """)
        ok(f"Tabela temporária criada: {tmp_cdr}")

        # Candidatos ±5 min e RN=1
        cur.execute(f"""
        CREATE TEMPORARY TABLE {tmp_conf} AS
        WITH candidatos AS (
            SELECT d.id AS detraf_id, c.id AS cdr_id,
                   TIMESTAMPDIFF(SECOND, d.data_hora, c.calldate) AS diff_sec,
                   d.eot_de_a, d.eot_de_b, c.EOT_A, c.EOT_B
            FROM {tmp_detraf} d
            JOIN {tmp_cdr} c
              ON d.a_short = c.src_short
             AND d.b_short = c.dst_short
             AND ABS(TIMESTAMPDIFF(MINUTE, d.data_hora, c.calldate)) <= 5
        ),
        ranqueados AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY detraf_id ORDER BY ABS(diff_sec)) AS rn
            FROM candidatos
        )
        SELECT * FROM ranqueados WHERE rn = 1
        """)
        ok(f"Matching concluído (RN=1) → {tmp_conf}")

        # Inserções: OK/PENDENTE
        cur.execute(f"""
        INSERT INTO detraf_conferencia (detraf_id, cdr_id, status, observacao)
        SELECT detraf_id, cdr_id,
               CASE
                 WHEN ( (EOT_A <=> eot_de_a) AND (EOT_B <=> eot_de_b) ) THEN 'OK'
                 ELSE 'PENDENTE'
               END AS status,
               CASE
                 WHEN ( (EOT_A <=> eot_de_a) AND (EOT_B <=> eot_de_b) ) THEN NULL
                 WHEN ( NOT (EOT_A <=> eot_de_a) AND NOT (EOT_B <=> eot_de_b) ) THEN 'EOT_A e EOT_B divergentes'
                 WHEN ( NOT (EOT_A <=> eot_de_a) ) THEN 'EOT_A divergente'
                 WHEN ( NOT (EOT_B <=> eot_de_b) ) THEN 'EOT_B divergente'
                 ELSE NULL
               END AS observacao
        FROM {tmp_conf}
        """)
        ok("Conferidos/Pendentes inseridos em detraf_conferencia.")

        # Inserções: PERDIDAS (sem match)
        cur.execute(f"""
        INSERT INTO detraf_conferencia (detraf_id, cdr_id, status, observacao)
        SELECT d.id AS detraf_id, NULL AS cdr_id, 'PERDIDA' AS status, 'Sem match ±5min' AS observacao
        FROM {tmp_detraf} d
        LEFT JOIN {tmp_conf} r ON r.detraf_id = d.id
        WHERE r.detraf_id IS NULL
        """)
        ok("Perdidos inseridos em detraf_conferencia.")

        conn.commit()

        # Drop temporárias
        cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_conf}")
        cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_cdr}")
        cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_detraf}")
        ok("Temporárias descartadas.")
