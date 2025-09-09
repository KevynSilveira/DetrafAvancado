from __future__ import annotations
import time
import pymysql
from .db import get_conn_params
from .env import load_env
from .log import info, ok, warn
from .normalizer import criar_tmp_cdr, criar_tmp_detraf

def _run_id() -> str:
    return time.strftime("%Y%m%d%H%M%S")

def processar_match() -> None:
    """Realiza o batimento entre ``detraf`` e ``cdr`` já importados."""
    load_env()
    params = get_conn_params()
    runid = _run_id()
    tmp_cdr = "cdr_normalizado"
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

        # tmp_detraf e cdr normalizado
        criar_tmp_detraf(cur, tmp_detraf, min_dt, max_dt)
        criar_tmp_cdr(cur, tmp_cdr, min_dt, max_dt)

        # Candidatos ±5 min e RN=1
        cur.execute(f"""
        CREATE TEMPORARY TABLE {tmp_conf} AS
        WITH candidatos AS (
            SELECT d.id AS detraf_id, c.id AS cdr_id,
                   TIMESTAMPDIFF(SECOND, d.data_hora, c.calldate) AS diff_sec,
                   d.eot_de_a, d.eot_de_b, c.EOT_A, c.EOT_B
            FROM {tmp_detraf} d
            JOIN {tmp_cdr} c
              ON d.a_num = c.src
             AND d.b_num = c.dst
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

        # Drop temporárias auxiliares
        cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_conf}")
        cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_detraf}")
        ok("Temporárias descartadas.")

        # View para conferência com colunas detalhadas
        cur.execute(
            f"""
            CREATE OR REPLACE VIEW detraf_conferencia_vw AS
            SELECT dc.id,
                   dc.status,
                   d.data_hora AS detraf_data_hora,
                   REGEXP_REPLACE(d.assinante_a_numero, '[^0-9]', '') AS assinante_a,
                   REGEXP_REPLACE(d.assinante_b_numero, '[^0-9]', '') AS assinante_b,
                   c.calldate AS cdr_data_hora,
                   d.eot_de_a AS detraf_eot_a,
                   d.eot_de_b AS detraf_eot_b,
                   NULL AS separador,
                   c.EOT_A AS cdr_eot_a,
                   c.EOT_B AS cdr_eot_b,
                   dc.observacao
            FROM detraf_conferencia dc
            JOIN detraf d ON dc.detraf_id = d.id
            LEFT JOIN {tmp_cdr} c ON dc.cdr_id = c.id
            """
        )
        ok("View detraf_conferencia_vw atualizada.")
