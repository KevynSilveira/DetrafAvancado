from __future__ import annotations
import time
import csv
from pathlib import Path
from datetime import datetime as _dt
import pymysql
from .db import get_conn_params
from .env import load_env
from .log import info, ok, warn
from .normalizer import criar_tmp_cdr, criar_tmp_detraf
from .normalizer import _resolve_eot  # usa validação em numeros_portados/cadup
from .normalizer import _lookup_eot_cadup  # busca direta em CADUP para perdidos

def _run_id() -> str:
    return time.strftime("%Y%m%d%H%M%S")

def processar_match() -> None:
    load_env()
    params = get_conn_params()
    runid = _run_id()
    tmp_cdr = "cdr_batimento_avancado"
    tmp_detraf = f"tmp_detraf_{runid}"
    tmp_conf = f"tmp_conf_{runid}"

    with pymysql.connect(**params) as conn:
        cur = conn.cursor()

        # Janela do DETRAF importado
        cur.execute("SELECT MIN(data_hora) AS min_dt, MAX(data_hora) AS max_dt, COUNT(*) AS total FROM detraf_arquivo_batimento_avancado")
        row = cur.fetchone()
        min_dt = row["min_dt"]; max_dt = row["max_dt"]; total_detraf = row["total"]
        if not total_detraf or not min_dt or not max_dt:
            warn("detraf_arquivo_batimento_avancado vazio ou sem data_hora. Nada a processar.")
            return
        info(f"Janela DETRAF detectada: {min_dt} → {max_dt} | {total_detraf} linhas")

        criar_tmp_detraf(cur, tmp_detraf, min_dt, max_dt)
        criar_tmp_cdr(cur, tmp_cdr, tmp_detraf, min_dt, max_dt)

        # Carrega contexto do período de referência (último registro)
        cur.execute("""
            SELECT periodo, ref_ini, ref_fim
            FROM detraf_context_batimento_avancado
            ORDER BY created_at DESC, id DESC
            LIMIT 1
        """)
        ctx = cur.fetchone()
        if not ctx:
            warn("Contexto do período não encontrado. Classificação de 'Recuperação de conta' será omitida.")
            ref_ini = None
            ref_fim = None
        else:
            ref_ini = ctx["ref_ini"]
            ref_fim = ctx["ref_fim"]

        # Candidatos ±5min e RN=1 — cria tabela explicitando tipos para evitar herdar defaults inválidos
        cur.execute(
            f"""
            DROP TEMPORARY TABLE IF EXISTS {tmp_conf}
            """
        )
        cur.execute(
            f"""
            CREATE TEMPORARY TABLE {tmp_conf} (
                detraf_id BIGINT,
                cdr_id BIGINT,
                diff_sec INT,
                detraf_dt DATETIME NULL,
                eot_de_a VARCHAR(32),
                eot_de_b VARCHAR(32),
                cdr_eot_a VARCHAR(32),
                cdr_eot_b VARCHAR(32),
                cdr_src VARCHAR(32),
                cdr_dst VARCHAR(32),
                disposition VARCHAR(32),
                calldate DATETIME NULL
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO {tmp_conf}
            SELECT detraf_id, cdr_id, diff_sec, detraf_dt,
                   eot_de_a, eot_de_b, cdr_eot_a, cdr_eot_b,
                   cdr_src, cdr_dst, disposition, calldate
            FROM (
                SELECT d.id AS detraf_id, c.id AS cdr_id,
                       TIMESTAMPDIFF(SECOND, d.data_hora, c.calldate) AS diff_sec,
                       d.data_hora AS detraf_dt,
                       d.eot_de_a, d.eot_de_b,
                       c.EOT_A AS cdr_eot_a, c.EOT_B AS cdr_eot_b,
                       c.src AS cdr_src, c.dst AS cdr_dst,
                       c.disposition, c.calldate,
                       ROW_NUMBER() OVER (PARTITION BY d.id ORDER BY ABS(TIMESTAMPDIFF(SECOND, d.data_hora, c.calldate))) AS rn
                FROM {tmp_detraf} d
                JOIN {tmp_cdr} c
                  ON d.a_num = c.src
                 AND d.b_num = c.dst
                 AND ABS(TIMESTAMPDIFF(MINUTE, d.data_hora, c.calldate)) <= 5
            ) ranked
            WHERE rn = 1
            """
        )
        ok(f"Matching concluído (RN=1) → {tmp_conf}")

        # Inserções (somente para pares com match RN=1), calculando EOT de referência sob demanda
        cur.execute(f"SELECT detraf_id, cdr_id, diff_sec, detraf_dt, eot_de_a, eot_de_b, cdr_eot_a, cdr_eot_b, cdr_src, cdr_dst, disposition, calldate FROM {tmp_conf}")
        rows = cur.fetchall()
        ins = []
        outdated_seen = set()
        outdated = []  # {'numero','eot_cdr','eot_correto','data_janela'}
        for r in rows:
            # acesso por chave ou índice
            get = (lambda k: r[k]) if isinstance(r, dict) else (lambda k: r[{
                'detraf_id':0,'cdr_id':1,'diff_sec':2,'detraf_dt':3,'eot_de_a':4,'eot_de_b':5,'cdr_eot_a':6,'cdr_eot_b':7,'cdr_src':8,'cdr_dst':9,'disposition':10,'calldate':11
            }[k]])
            detraf_id = get('detraf_id'); cdr_id = get('cdr_id')
            detraf_dt = get('detraf_dt'); disp = get('disposition')
            eot_bat_a = get('eot_de_a'); eot_bat_b = get('eot_de_b')
            cdr_src = str(get('cdr_src')); cdr_dst = str(get('cdr_dst'))
            cdr_eot_a = (str(get('cdr_eot_a')) if get('cdr_eot_a') is not None else None)
            cdr_eot_b = (str(get('cdr_eot_b')) if get('cdr_eot_b') is not None else None)
            calldate = get('calldate')

            # Resolve EOT de referência sob demanda
            eot_ref_a, origem_a, port_a = _resolve_eot(cur, cdr_src, calldate)
            eot_ref_b, origem_b, port_b = _resolve_eot(cur, cdr_dst, calldate)
            if eot_ref_a is None:
                eot_ref_a = cdr_eot_a
            if eot_ref_b is None:
                eot_ref_b = cdr_eot_b

            # Coleta para relatório de desatualizados (independente de Operadora vs CDR, inclui não atendidas)
            try:
                if (eot_ref_a is not None and eot_ref_a != cdr_eot_a
                    and (port_a is None or (calldate and port_a <= calldate))):
                    key = (cdr_src, cdr_eot_a, eot_ref_a)
                    if key not in outdated_seen:
                        outdated_seen.add(key)
                        outdated.append({
                            'numero': cdr_src,
                            'eot_cdr': cdr_eot_a or '',
                            'eot_correto': eot_ref_a or '',
                            'data_janela': port_a.strftime('%Y-%m-%d %H:%M:%S') if getattr(port_a, 'strftime', None) else (str(port_a) if port_a is not None else ''),
                        })
                if (eot_ref_b is not None and eot_ref_b != cdr_eot_b
                    and (port_b is None or (calldate and port_b <= calldate))):
                    key = (cdr_dst, cdr_eot_b, eot_ref_b)
                    if key not in outdated_seen:
                        outdated_seen.add(key)
                        outdated.append({
                            'numero': cdr_dst,
                            'eot_cdr': cdr_eot_b or '',
                            'eot_correto': eot_ref_b or '',
                            'data_janela': port_b.strftime('%Y-%m-%d %H:%M:%S') if getattr(port_b, 'strftime', None) else (str(port_b) if port_b is not None else ''),
                        })
            except Exception:
                # Não interrompe coleta por problemas pontuais de tipos
                pass

            # Carrega portabilidade mais recente (independente da data) para contextualizar observação
            np_a_eot = None; np_a_dt = None
            np_b_eot = None; np_b_dt = None
            try:
                cur.execute(
                    "SELECT eot, data_janela FROM numeros_portados WHERE numero = %s ORDER BY data_janela DESC LIMIT 1",
                    (cdr_src,)
                )
                row_np = cur.fetchone()
                if row_np:
                    np_a_eot = (row_np.get('eot') if isinstance(row_np, dict) else row_np[0])
                    np_a_eot = str(np_a_eot) if np_a_eot is not None else None
                    np_a_dt = (row_np.get('data_janela') if isinstance(row_np, dict) else row_np[1])
            except Exception:
                pass
            try:
                cur.execute(
                    "SELECT eot, data_janela FROM numeros_portados WHERE numero = %s ORDER BY data_janela DESC LIMIT 1",
                    (cdr_dst,)
                )
                row_np = cur.fetchone()
                if row_np:
                    np_b_eot = (row_np.get('eot') if isinstance(row_np, dict) else row_np[0])
                    np_b_eot = str(np_b_eot) if np_b_eot is not None else None
                    np_b_dt = (row_np.get('data_janela') if isinstance(row_np, dict) else row_np[1])
            except Exception:
                pass

            answered = (disp is None) or (str(disp).upper() == 'ANSWERED')
            eot_ok = ( (eot_ref_a == eot_bat_a) and (eot_ref_b == eot_bat_b) )
            status = 'Conferência' if (answered and eot_ok) else 'Erro'

            obs_parts = []
            if not answered:
                obs_parts.append(f"CDR nao atendido (disposition={str(disp).upper()})")
            # Só valida EOT quando a chamada foi atendida
            if answered:
                # Lado A: primeiro foco em Operadora vs CDR; depois nota adicional sobre nossa base
                if eot_bat_a != cdr_eot_a:
                    # Caso especial: referência (NP/CADUP) difere do CDR no período → CDR desatualizado
                    if (eot_ref_a is not None and eot_ref_a != cdr_eot_a
                        and (port_a is None or (detraf_dt and port_a <= detraf_dt))):
                        when = ''
                        if port_a:
                            try:
                                when = f" (portado em {port_a.strftime('%Y-%m-%d')})"
                            except Exception:
                                when = f" (portado em {port_a})"
                        fonte = 'Números Portados' if (origem_a == 'numeros_portados') else 'CADUP'
                        obs_parts.append(f"CDR desatualizado no período da chamada. EOT do CDR={cdr_eot_a or 'NULL'}; {fonte}={eot_ref_a}{when}")
                        # Coleta para relatório de desatualizados (sem distinção A/B)
                        key = (cdr_src, cdr_eot_a, eot_ref_a)
                        if key not in outdated_seen:
                            outdated_seen.add(key)
                            outdated.append({
                                'numero': cdr_src,
                                'eot_cdr': cdr_eot_a or '',
                                'eot_correto': eot_ref_a or '',
                                'data_janela': port_a.strftime('%Y-%m-%d %H:%M:%S') if getattr(port_a, 'strftime', None) else (str(port_a) if port_a is not None else ''),
                            })
                    else:
                        # Mensagem padrão, sem listar todas as fontes
                        obs_parts.append(f"EOT_A divergente entre operadora={eot_bat_a or 'NULL'} e CDR={cdr_eot_a or 'NULL'}")
                else:
                    # Operadora e CDR batem; se nossa base diverge, informar (com 'desde' quando disponível)
                    try:
                        if np_a_eot and np_a_eot != cdr_eot_a:
                            if np_a_dt and detraf_dt and np_a_dt <= detraf_dt:
                                when = np_a_dt.strftime('%Y-%m-%d') if hasattr(np_a_dt, 'strftime') else str(np_a_dt)
                                obs_parts.append(f"EOT_A: Operadora e CDR concordam (= {cdr_eot_a}), porém nossa base (numeros_portados) indica {np_a_eot} desde {when}")
                            elif np_a_dt is None:
                                obs_parts.append(f"EOT_A: Operadora e CDR concordam (= {cdr_eot_a}), porém nossa base (numeros_portados) indica {np_a_eot}")
                    except Exception:
                        pass
                # Lado B
                if eot_bat_b != cdr_eot_b:
                    # Caso especial: referência (NP/CADUP) difere do CDR no período → CDR desatualizado
                    if (eot_ref_b is not None and eot_ref_b != cdr_eot_b
                        and (port_b is None or (detraf_dt and port_b <= detraf_dt))):
                        when = ''
                        if port_b:
                            try:
                                when = f" (portado em {port_b.strftime('%Y-%m-%d')})"
                            except Exception:
                                when = f" (portado em {port_b})"
                        fonte = 'Números Portados' if (origem_b == 'numeros_portados') else 'CADUP'
                        obs_parts.append(f"CDR desatualizado no período da chamada. EOT do CDR={cdr_eot_b or 'NULL'}; {fonte}={eot_ref_b}{when}")
                        key = (cdr_dst, cdr_eot_b, eot_ref_b)
                        if key not in outdated_seen:
                            outdated_seen.add(key)
                            outdated.append({
                                'numero': cdr_dst,
                                'eot_cdr': cdr_eot_b or '',
                                'eot_correto': eot_ref_b or '',
                                'data_janela': port_b.strftime('%Y-%m-%d %H:%M:%S') if getattr(port_b, 'strftime', None) else (str(port_b) if port_b is not None else ''),
                            })
                    else:
                        obs_parts.append(f"EOT_B divergente entre operadora={eot_bat_b or 'NULL'} e CDR={cdr_eot_b or 'NULL'}")
                else:
                    try:
                        if np_b_eot and np_b_eot != cdr_eot_b:
                            if np_b_dt and detraf_dt and np_b_dt <= detraf_dt:
                                when = np_b_dt.strftime('%Y-%m-%d') if hasattr(np_b_dt, 'strftime') else str(np_b_dt)
                                obs_parts.append(f"EOT_B: Operadora e CDR concordam (= {cdr_eot_b}), porém nossa base (numeros_portados) indica {np_b_eot} desde {when}")
                            elif np_b_dt is None:
                                obs_parts.append(f"EOT_B: Operadora e CDR concordam (= {cdr_eot_b}), porém nossa base (numeros_portados) indica {np_b_eot}")
                    except Exception:
                        pass
            if ref_ini and ref_fim:
                try:
                    if detraf_dt < ref_ini or detraf_dt > ref_fim:
                        obs_parts.append('RECUPERACAO_DE_CONTA')
                except Exception:
                    pass

            observacao = ' | '.join([p for p in obs_parts if p]) or None
            ins.append((detraf_id, cdr_id, status, observacao))

        if ins:
            cur.executemany(
                "INSERT INTO detraf_processado_batimento_avancado (detraf_id, cdr_id, status, observacao) VALUES (%s,%s,%s,%s)",
                ins,
            )
        ok("Conferidos/Erros inseridos em detraf_processado_batimento_avancado.")

        if ref_ini and ref_fim:
            cur.execute(f"""
            INSERT INTO detraf_processado_batimento_avancado (detraf_id, cdr_id, status, observacao)
            SELECT d.id AS detraf_id, NULL AS cdr_id, 'Perdido' AS status,
                   CONCAT('Sem match ±5min', CASE WHEN (d.data_hora < %s OR d.data_hora > %s) THEN ' | RECUPERACAO_DE_CONTA' ELSE '' END) AS observacao
            FROM {tmp_detraf} d
            LEFT JOIN {tmp_conf} r ON r.detraf_id = d.id
            WHERE r.detraf_id IS NULL
            """, (ref_ini, ref_fim))
        else:
            cur.execute(f"""
            INSERT INTO detraf_processado_batimento_avancado (detraf_id, cdr_id, status, observacao)
            SELECT d.id AS detraf_id, NULL AS cdr_id, 'Perdido' AS status, 'Sem match ±5min' AS observacao
            FROM {tmp_detraf} d
            LEFT JOIN {tmp_conf} r ON r.detraf_id = d.id
            WHERE r.detraf_id IS NULL
            """)
        ok("Perdidas inseridas em detraf_processado_batimento_avancado.")

        # Enriquecimento das PERDIDAS com sugestão de EOT via CADUP
        try:
            cur.execute(f"""
                SELECT d.id AS detraf_id, d.a_num, d.b_num, d.data_hora
                FROM {tmp_detraf} d
                LEFT JOIN {tmp_conf} r ON r.detraf_id = d.id
                WHERE r.detraf_id IS NULL
            """)
            perdidos = cur.fetchall() or []
            updates = []
            for row in perdidos:
                if isinstance(row, dict):
                    detraf_id = row.get('detraf_id')
                    a_num = row.get('a_num'); b_num = row.get('b_num')
                else:
                    detraf_id, a_num, b_num, _ = row
                eot_a, src_a = _lookup_eot_cadup(cur, str(a_num) if a_num is not None else '')
                eot_b, src_b = _lookup_eot_cadup(cur, str(b_num) if b_num is not None else '')
                parts = []
                if eot_a:
                    parts.append(f"CADUP sugerido EOT_A={eot_a}")
                if eot_b:
                    parts.append(f"CADUP sugerido EOT_B={eot_b}")
                if parts and detraf_id:
                    updates.append((" | ".join(parts), int(detraf_id)))
            if updates:
                # Aplica atualização nas observações mantendo o texto existente
                cur.executemany(
                    """
                    UPDATE detraf_processado_batimento_avancado
                    SET observacao = CASE WHEN (observacao IS NULL OR observacao = '')
                                           THEN %s
                                           ELSE CONCAT(observacao, ' | ', %s)
                                      END
                    WHERE detraf_id = %s AND cdr_id IS NULL
                    """,
                    [(txt, txt, did) for (txt, did) in updates]
                )
        except Exception as _ex:
            # Enriquecimento é best-effort; não interromper pipeline
            pass

        conn.commit()

        # cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_conf}")
        # cur.execute(f"DROP TEMPORARY TABLE IF EXISTS {tmp_detraf}")
        ok("Temporárias descartadas.")

        # View final (escapa % no literal)
        # View sempre baseada no contexto salvo (sem embutir datas)
        cur.execute(
            f"""
            CREATE OR REPLACE VIEW detraf_batimento_avancado_vw AS
            SELECT dc.id AS ID,
                   CASE
                     WHEN dc.cdr_id IS NULL THEN 'Perdido'
                     WHEN (c.disposition IS NOT NULL AND UPPER(c.disposition) <> 'ANSWERED') THEN 'Erro'
                     WHEN ( (c.EOT_A <=> d.eot_de_a) AND (c.EOT_B <=> d.eot_de_b) ) THEN 'Conferência'
                     ELSE 'Erro'
                   END AS STATUS,
                   CASE
                     WHEN dc.cdr_id IS NULL THEN NULL
                     ELSE CONCAT(LPAD(FLOOR(ABS(TIMESTAMPDIFF(SECOND, d.data_hora, c.calldate))/60), 2, '0'), ':', LPAD(MOD(ABS(TIMESTAMPDIFF(SECOND, d.data_hora, c.calldate)), 60), 2, '0'))
                   END AS diferenca_tempo,
                   DATE_FORMAT(d.data_hora, '%Y-%m-%d %H:%i:%s') AS Data_hora_batimento,
                   CASE
                     WHEN LENGTH(REGEXP_REPLACE(d.assinante_a_numero, '[^0-9]', '')) IN (12,13)
                          THEN SUBSTRING(REGEXP_REPLACE(d.assinante_a_numero, '[^0-9]', ''), 3)
                     ELSE REGEXP_REPLACE(d.assinante_a_numero, '[^0-9]', '')
                   END AS `origem batimento`,
                   CASE
                     WHEN LENGTH(REGEXP_REPLACE(d.assinante_b_numero, '[^0-9]', '')) IN (12,13)
                          THEN SUBSTRING(REGEXP_REPLACE(d.assinante_b_numero, '[^0-9]', ''), 3)
                     ELSE REGEXP_REPLACE(d.assinante_b_numero, '[^0-9]', '')
                   END AS `destino batimento`,
                   d.eot_de_a AS EOT_A_Batimento,
                   d.eot_de_b AS EOT_B_Batimento,
                   dc.cdr_id AS id_cdr,
                   c.EOT_A AS cdr_eot_A,
                   c.EOT_B AS cdr_eot_B,
                   CASE
                     WHEN dc.cdr_id IS NULL THEN 4
                     WHEN (c.disposition IS NOT NULL AND UPPER(c.disposition) <> 'ANSWERED') THEN 1
                     WHEN ((c.EOT_A IS NOT NULL AND d.eot_de_a IS NOT NULL AND c.EOT_A <> d.eot_de_a)
                           AND (c.EOT_B IS NOT NULL AND d.eot_de_b IS NOT NULL AND c.EOT_B <> d.eot_de_b)) THEN 5
                     WHEN (c.EOT_A IS NOT NULL AND d.eot_de_a IS NOT NULL AND c.EOT_A <> d.eot_de_a) THEN 3
                     WHEN (c.EOT_B IS NOT NULL AND d.eot_de_b IS NOT NULL AND c.EOT_B <> d.eot_de_b) THEN 2
                     ELSE NULL
                   END AS codigo_erro,
                   dc.observacao
            FROM detraf_processado_batimento_avancado dc
            JOIN detraf_arquivo_batimento_avancado d ON dc.detraf_id = d.id
            LEFT JOIN cdr c ON c.id = dc.cdr_id
            ORDER BY FIELD(STATUS, 'Conferência','Erro','Perdido'), d.data_hora, codigo_erro
            """
        )
        ok("View detraf_batimento_avancado_vw atualizada.")

        # Geração do CSV de números desatualizados com base somente nas linhas com match
        try:
            ts = _dt.now().strftime('%Y%m%d_%H%M%S')
            out_dir = Path('build'); out_dir.mkdir(parents=True, exist_ok=True)
            f_path = out_dir / f'desatualizados_{ts}.csv'
            with f_path.open('w', newline='', encoding='utf-8') as fh:
                w = csv.DictWriter(fh, fieldnames=['numero','eot_cdr','eot_correto','data_janela'])
                w.writeheader(); w.writerows(outdated or [])
            ok(f"CSV gerado: {f_path} ({len(outdated or [])} linhas)")
        except Exception as _ex:
            # Não interrompe o pipeline se falhar o relatório auxiliar
            warn(f"Falha ao gerar lista de desatualizados: {_ex}")
