#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime
from calendar import monthrange

CONFIG_PATH = Path.home() / ".detraf_cli.json"
from .env import CONFIGS_DIR
from .normalizer import _resolve_eot  # para obter EOT de referência (numeros_portados/cadup)
from .normalizer import _lookup_eot_cadup  # fallback direto para CADUP
LAYOUT_YAML = str((CONFIGS_DIR / "detraf_layout.yaml").resolve())

# ---------- util ----------
def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def ok(msg: str) -> None:
    print(f"{ts()} OK {msg}")

def warn(msg: str) -> None:
    print(f"{ts()} AVISO {msg}")

def err(msg: str) -> None:
    print(f"{ts()} ERRO {msg}")

def load_cfg() -> dict:
    if CONFIG_PATH.exists():
        try:
            return json.loads(CONFIG_PATH.read_text(encoding="utf-8") or "{}")
        except Exception:
            return {}
    return {}

def save_cfg(d: dict) -> None:
    CONFIG_PATH.write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")

def ensure_db_env_or_fail() -> None:
    cfg = load_cfg()
    env_ok = all(os.getenv(k) for k in ("DB_HOST", "DB_USER", "DB_NAME"))
    if env_ok:
        return
    db = cfg.get("db", {})
    if db.get("host") and db.get("user") and db.get("name"):
        os.environ.setdefault("DB_HOST", db.get("host"))
        os.environ.setdefault("DB_PORT", str(db.get("port", 3306)))
        os.environ.setdefault("DB_USER", db.get("user"))
        os.environ.setdefault("DB_PASSWORD", db.get("password", ""))
        os.environ.setdefault("DB_NAME", db.get("name"))
        return
    print("\n╭────────────────────────────╮\n│ VALIDAÇÃO INICIAL DO BANCO │\n╰────────────────────────────╯")
    err("Configuração de banco ausente. Rode: detraf db-config")
    sys.exit(1)

def month_window_yyyymm(yyyymm: str) -> tuple[str, str]:
    ano = int(yyyymm[:4]); mes = int(yyyymm[4:6])
    last = monthrange(ano, mes)[1]
    ini = f"{ano:04d}-{mes:02d}-01 00:00:00"
    fim = f"{ano:04d}-{mes:02d}-{last:02d} 23:59:59"
    return ini, fim

def truncate_tables_fallback() -> None:
    # Cria/garante as tabelas do batimento e limpa para novo processamento
    import pymysql
    params = dict(
        host=os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT","3306")),
        user=os.getenv("DB_USER","root"),
        password=os.getenv("DB_PASSWORD",""),
        database=os.getenv("DB_NAME",""),
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    conn = pymysql.connect(**params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS detraf_arquivo_batimento_avancado (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    eot VARCHAR(10) NOT NULL,
                    sequencial BIGINT,
                    assinante_a_numero VARCHAR(32),
                    eot_de_a VARCHAR(10),
                    cnl_de_a VARCHAR(10),
                    area_local_de_a VARCHAR(10),
                    data_da_chamada CHAR(8),
                    hora_de_atendimento CHAR(6),
                    assinante_b_numero VARCHAR(32),
                    eot_de_b VARCHAR(10),
                    cnl_de_b VARCHAR(10),
                    area_local_de_b VARCHAR(10),
                    data_hora DATETIME,
                    INDEX idx_detraf_data_hora (data_hora)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS detraf_processado_batimento_avancado (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    detraf_id BIGINT NOT NULL,
                    cdr_id BIGINT NULL,
                    status VARCHAR(20) NOT NULL,
                    observacao VARCHAR(255) NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    INDEX idx_detraf_id (detraf_id),
                    INDEX idx_cdr_id (cdr_id)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS codigo_erro_batimento_avancado (
                    codigo INT PRIMARY KEY,
                    descricao VARCHAR(255) NOT NULL,
                    ativo TINYINT NOT NULL DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            # Contexto do período de referência (para classificação e view)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS detraf_context_batimento_avancado (
                    id BIGINT AUTO_INCREMENT PRIMARY KEY,
                    periodo CHAR(6) NOT NULL,
                    ref_ini DATETIME NOT NULL,
                    ref_fim DATETIME NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                """
            )
            cur.execute(
                """
                INSERT IGNORE INTO codigo_erro_batimento_avancado (codigo, descricao, ativo) VALUES
                  (1,'Cobranca indevida - chamada com disposition diferente de atendida.',1),
                  (2,'EOT de B do batimento diferente do EOT de B do CDR.',1),
                  (3,'EOT de A do batimento diferente do EOT de A do CDR.',1),
                  (4,'Chamada do batimento nao encontrado no CDR.',1),
                  (5,'EOT de A e de B do batimento nao bate com o CDR.',1)
                """
            )
            truncated = []
            cur.execute("TRUNCATE TABLE detraf_arquivo_batimento_avancado"); truncated.append("detraf_arquivo_batimento_avancado")
            cur.execute("TRUNCATE TABLE detraf_processado_batimento_avancado"); truncated.append("detraf_processado_batimento_avancado")
            try:
                cur.execute("TRUNCATE TABLE detraf_context_batimento_avancado"); truncated.append("detraf_context_batimento_avancado")
            except Exception:
                pass
            ok(f"Truncadas: {', '.join(truncated)}")
            cur.execute("TRUNCATE TABLE detraf_context_batimento_avancado")
    finally:
        conn.close()

def db_select_1() -> bool:
    import pymysql
    try:
        params = dict(
            host=os.getenv("DB_HOST","localhost"),
            port=int(os.getenv("DB_PORT","3306")),
            user=os.getenv("DB_USER","root"),
            # Correção: usar a mesma chave de senha usada no restante do projeto
            password=os.getenv("DB_PASSWORD",""),
            database=os.getenv("DB_NAME",""),
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        conn = pymysql.connect(**params)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                row = cur.fetchone()
                return bool(row and list(row.values())[0] == 1)
        finally:
            conn.close()
    except Exception:
        return False

# ---------- comandos ----------
def cmd_db_check(_args: argparse.Namespace) -> int:
    if not all(os.getenv(k) for k in ("DB_HOST","DB_USER","DB_NAME")) and not load_cfg().get("db"):
        err("Configuração de banco ausente. Rode: detraf db-config")
        return 1
    ensure_db_env_or_fail()
    if db_select_1():
        ok("Conexão OK (SELECT 1 -> 1)")
        return 0
    err("Falha na conexão ao banco.")
    return 1

def _export_csvs(periodo: str) -> None:
    """Gera CSVs (batimento, detalhado, sintético) em build/.

    - batimento_<ts>.csv: colunas essenciais para cliente
    - detalhado_<ts>.csv: visão com campos técnicos e motivo
    - sintetico_<ts>.csv: resumo por categoria e código de erro
    """
    import csv
    import pymysql
    from datetime import datetime as _dt

    ts = _dt.now().strftime("%Y%m%d_%H%M%S")
    out_dir = Path("build"); out_dir.mkdir(parents=True, exist_ok=True)
    f_bat = out_dir / f"batimento_{ts}.csv"
    f_det = out_dir / f"detalhado_{ts}.csv"
    f_sin = out_dir / f"sintetico_{ts}.csv"

    params = dict(
        host=os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT","3306")),
        user=os.getenv("DB_USER","root"),
        password=os.getenv("DB_PASSWORD",""),
        database=os.getenv("DB_NAME",""),
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    conn = pymysql.connect(**params)
    try:
        with conn.cursor() as cur:
            # Batimento
            cur.execute(
                """
                SELECT STATUS,
                       diferenca_tempo,
                       Data_hora_batimento,
                       `origem batimento` AS origem,
                       `destino batimento` AS destino,
                       EOT_A_Batimento,
                       EOT_B_Batimento
                FROM detraf_batimento_avancado_vw
                ORDER BY FIELD(STATUS,'Conferência','Erro','Perdido'), Data_hora_batimento, codigo_erro
                """
            )
            rows = cur.fetchall()
            if rows:
                with f_bat.open("w", newline="", encoding="utf-8") as fh:
                    w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
                    w.writeheader(); w.writerows(rows)
                ok(f"CSV gerado: {f_bat} ({len(rows)} linhas)")

            # Detalhado
            cur.execute(
                """
                SELECT STATUS,
                       diferenca_tempo,
                       Data_hora_batimento,
                       `origem batimento` AS origem,
                       `destino batimento` AS destino,
                       EOT_A_Batimento,
                       EOT_B_Batimento,
                       id_cdr,
                       cdr_eot_A,
                       cdr_eot_B,
                       codigo_erro,
                       observacao
                FROM detraf_batimento_avancado_vw
                ORDER BY FIELD(STATUS,'Conferência','Erro','Perdido'), Data_hora_batimento, codigo_erro
                """
            )
            rows = cur.fetchall()
            # Enriquecer com EOT de referência (numeros_portados/cadup) por lado A/B
            if rows:
                # Mapa de CDR para reduzir roundtrips (id -> (calldate, src, dst))
                ids = [r["id_cdr"] for r in rows if r.get("id_cdr")]
                cdr_map = {}
                if ids:
                    # fatiar para evitar IN muito grande
                    for i in range(0, len(ids), 1000):
                        chunk = ids[i:i+1000]
                        fmt = ",".join(["%s"] * len(chunk))
                        cur.execute(f"SELECT id, calldate, src, dst FROM cdr WHERE id IN ({fmt})", tuple(chunk))
                        for rr in cur.fetchall():
                            cid = rr["id"] if isinstance(rr, dict) else rr[0]
                            calld = rr["calldate"] if isinstance(rr, dict) else rr[1]
                            srcn = rr["src"] if isinstance(rr, dict) else rr[2]
                            dstn = rr["dst"] if isinstance(rr, dict) else rr[3]
                            cdr_map[int(cid)] = (calld, str(srcn), str(dstn))

                # Computa ref_eot_A/B por linha
                for r in rows:
                    rid = r.get("id_cdr")
                    ref_a = None; ref_b = None
                    if rid and int(rid) in cdr_map:
                        calld, srcn, dstn = cdr_map[int(rid)]
                        ref_a, _, _ = _resolve_eot(cur, srcn, calld)
                        ref_b, _, _ = _resolve_eot(cur, dstn, calld)
                        # Fallback direto: se ainda não veio, tenta CADUP puro
                        # Fallback CADUP usando os números do batimento (mais confiáveis para origem/destino)
                        if not ref_a and r.get("origem"):
                            ref_a, _ = _lookup_eot_cadup(cur, str(r.get("origem")))
                        if not ref_b and r.get("destino"):
                            ref_b, _ = _lookup_eot_cadup(cur, str(r.get("destino")))
                    r["ref_eot_A"] = ref_a
                    r["ref_eot_B"] = ref_b

                with f_det.open("w", newline="", encoding="utf-8") as fh:
                    # Ordem explícita de colunas
                    fieldnames = [
                        "STATUS","diferenca_tempo","Data_hora_batimento","origem","destino",
                        "EOT_A_Batimento","EOT_B_Batimento","id_cdr","cdr_eot_A","cdr_eot_B",
                        "ref_eot_A","ref_eot_B","codigo_erro","observacao"
                    ]
                    w = csv.DictWriter(fh, fieldnames=fieldnames)
                    w.writeheader(); w.writerows(rows)
                ok(f"CSV gerado: {f_det} ({len(rows)} linhas)")

            # Sintético (organizado conforme solicitado)
            sintetico_rows: list[dict] = []

            # Totais por categoria
            cur.execute(
                """
                SELECT STATUS AS categoria, COUNT(*) AS total
                FROM detraf_batimento_avancado_vw
                GROUP BY STATUS
                """
            )
            cat_totais = {r["categoria"]: r["total"] for r in cur.fetchall()}

            # Total de erros
            cur.execute("SELECT COUNT(*) AS total FROM detraf_batimento_avancado_vw WHERE STATUS='Erro'")
            erros_total = int((cur.fetchone() or {}).get("total", 0))

            # Erros por código (inclui códigos sem ocorrência = 0)
            cur.execute(
                """
                SELECT ce.codigo AS codigo_erro,
                       ce.descricao AS descricao,
                       COALESCE(cnt.total, 0) AS total
                FROM codigo_erro_batimento_avancado ce
                LEFT JOIN (
                    SELECT codigo_erro, COUNT(*) AS total
                    FROM detraf_batimento_avancado_vw
                    WHERE STATUS = 'Erro'
                    GROUP BY codigo_erro
                ) cnt ON cnt.codigo_erro = ce.codigo
                ORDER BY ce.codigo
                """
            )
            erros = cur.fetchall()

            # Recuperação de conta (flag na observação)
            cur.execute(
                """
                SELECT COUNT(*) AS total
                FROM detraf_batimento_avancado_vw
                WHERE observacao LIKE '%RECUPERACAO_DE_CONTA%'
                """
            )
            rec_count = int((cur.fetchone() or {}).get("total", 0))

            # Helper para capitalizar a primeira letra
            def _cap_first(s: str | None) -> str | None:
                if not s:
                    return s
                return s[:1].upper() + s[1:]

            # 1) Conferência
            sintetico_rows.append({
                "categoria": "Conferência", "codigo_erro": None, "descricao": None,
                "total": int(cat_totais.get("Conferência", 0)),
            })
            # 2) Perdidos
            sintetico_rows.append({
                "categoria": "Perdidos", "codigo_erro": None, "descricao": None,
                "total": int(cat_totais.get("Perdido", 0)),
            })
            # 3) Recuperação de contas
            sintetico_rows.append({
                "categoria": "Recuperação de Contas", "codigo_erro": None, "descricao": "Registros fora do mês de referência.",
                "total": rec_count,
            })
            # 4) Erros total
            sintetico_rows.append({
                "categoria": "Erros Total", "codigo_erro": None, "descricao": None,
                "total": erros_total,
            })
            # 5) Erro 01..05 (categoria rotulada "Erro 0X")
            for e in erros:
                code = int(e.get("codigo_erro", 0)) if e.get("codigo_erro") is not None else 0
                label = f"Erro {code:02d}" if code else "Erro"
                desc_cap = _cap_first(e.get("descricao")) if e.get("descricao") is not None else None
                sintetico_rows.append({
                    "categoria": f"{label} - {desc_cap}" if desc_cap else label,
                    "codigo_erro": label if code else None,
                    "descricao": desc_cap,
                    "total": int(e.get("total", 0)),
                })

            # Grava o CSV do sintético: apenas 2 colunas (sem cabeçalho): nome, total
            if sintetico_rows:
                with f_sin.open("w", newline="", encoding="utf-8") as fh:
                    w = csv.writer(fh)
                    for r in sintetico_rows:
                        w.writerow([r.get("categoria"), r.get("total")])
                ok(f"CSV gerado: {f_sin} ({len(sintetico_rows)} linhas)")
    finally:
        conn.close()

def cmd_db_config(_args: argparse.Namespace) -> int:
    print("\n-- CONFIGURAÇÃO DO BANCO")
    cfg = load_cfg()
    db = cfg.get("db", {})
    host = input(f"Host [{db.get('host','localhost')}]: ") or db.get("host","localhost")
    port_raw = input(f"Port [{db.get('port',3306)}]: ") or str(db.get("port",3306))
    user = input(f"User [{db.get('user','root')}]: ") or db.get("user","root")
    password = input(f"Password [{'*'*len(db.get('password',''))}]: ") or db.get("password","")
    name = input(f"Database [{db.get('name','spx_o')}]: ") or db.get("name","spx_o")
    try:
        port = int(port_raw)
    except ValueError:
        err("Port inválida.")
        return 1
    cfg["db"] = {"host": host, "port": port, "user": user, "password": password, "name": name}
    save_cfg(cfg)
    ok("Banco configurado.")
    return 0

def cmd_config(_args: argparse.Namespace) -> int:
    print("\n-- CONFIGURAÇÃO DO DETRAF")
    print("Defina período, EOT e caminho do arquivo DETRAF.")
    cfg = load_cfg()
    periodo = input(f"Período (YYYYMM) [{cfg.get('periodo','')}]: ") or cfg.get("periodo","")
    eot = input(f"EOT (3 dígitos) [{cfg.get('eot','010')}]: ") or cfg.get("eot","010")
    arquivo = input(f"Caminho do arquivo DETRAF [{cfg.get('arquivo','data/Detraf.txt')}]: ") or cfg.get("arquivo","data/Detraf.txt")
    if not periodo or len(periodo)!=6 or not periodo.isdigit():
        err("Período inválido. Use YYYYMM.")
        return 1
    if not eot or len(eot)!=3:
        err("EOT inválido. Use 3 dígitos.")
        return 1
    cfg["periodo"] = periodo
    cfg["eot"] = eot
    cfg["arquivo"] = arquivo
    save_cfg(cfg)
    ok("Configuração básica concluída.")
    return 0

def cmd_run(args: argparse.Namespace) -> int:
    # 1) DB obrigatoriamente configurado (env ou cfg); senão encerra
    ensure_db_env_or_fail()
    if not db_select_1():
        err("Falha na conexão ao banco.")
        return 1
    ok("Conexão OK (SELECT 1 -> 1)")

    # 2) Coleta das variáveis: se --config, pergunta e salva; senão, usa cfg (sem perguntar)
    cfg = load_cfg()
    if args.config:
        # configurar durante a execução
        _ = cmd_config(args)
        cfg = load_cfg()
    periodo = cfg.get("periodo")
    eot = cfg.get("eot")
    arquivo = cfg.get("arquivo")
    if not (periodo and eot and arquivo):
        err("Variáveis ausentes. Rode: detraf config  ou use: detraf run --config")
        return 1

    ok(f"periodo = {periodo}")
    ok(f"eot = {eot}")
    ok(f"arquivo = {arquivo}")

    # 3) Contexto de cobrança (janela do mês)
    janela_ini, janela_fim = month_window_yyyymm(periodo)
    ok(f"janela_referencia_ini = {janela_ini}")
    ok(f"janela_referencia_fim = {janela_fim}")
    ok("Regra: importar todas as linhas; fora do mês = RECUPERAÇÃO DE CONTA.")

    # 4) Preparação de banco (truncate)
    try:
        # se existir util do projeto, usa; senão, fallback
        try:
            from .processing import truncate_tables  # type: ignore
            truncate_tables()
        except Exception:
            truncate_tables_fallback()
        # Logs de truncates são emitidos dentro do helper
        # Salva contexto do período para a etapa de matching (usado pela view e classificações)
        try:
            import pymysql
            params = dict(
                host=os.getenv("DB_HOST","localhost"),
                port=int(os.getenv("DB_PORT","3306")),
                user=os.getenv("DB_USER","root"),
                password=os.getenv("DB_PASSWORD",""),
                database=os.getenv("DB_NAME",""),
                autocommit=True,
                cursorclass=pymysql.cursors.DictCursor,
            )
            conn = pymysql.connect(**params)
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS detraf_context_batimento_avancado (
                        id BIGINT AUTO_INCREMENT PRIMARY KEY,
                        periodo CHAR(6) NOT NULL,
                        ref_ini DATETIME NOT NULL,
                        ref_fim DATETIME NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
                    """
                )
                cur.execute("TRUNCATE TABLE detraf_context_batimento_avancado")
                cur.execute(
                    "INSERT INTO detraf_context_batimento_avancado (periodo, ref_ini, ref_fim) VALUES (%s,%s,%s)",
                    (periodo, janela_ini, janela_fim),
                )
            ok("Contexto do período gravado.")
        except Exception as ex:
            err(f"Falha ao salvar contexto do período: {ex}")
            return 1
        ok("Preparação concluída.")
    except Exception as ex:
        err(f"Falha ao preparar o banco: {ex}")
        return 1

    # 5) Importação DETRAF
    ok("Iniciando importação do arquivo...")
    try:
        from .import_detraf import importar_arquivo_txt
    except Exception as ex:
        err(f"Falha ao carregar importação: {ex}")
        return 1

    try:
        _ = importar_arquivo_txt(arquivo, periodo, eot, layout_path=LAYOUT_YAML)
    except Exception as ex:
        err(f"Falha na importação do arquivo: {ex}")
        raise

    # 6) Próximas etapas (normalização/comparação) — delega ao projeto se existirem
    try:
        from .processing import processar_match  # type: ignore
        ok("Importação concluída. Iniciando matching...")
        processar_match()
        ok("Matching concluído.")
        _export_csvs(periodo)
        ok("Processo finalizado.")
    except Exception:
        # Se o projeto não tiver essas rotinas, apenas finalizar.
        ok("Pós-importação não encontrada no módulo. Fim da execução.")
    return 0

# ---------- parser ----------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="detraf", description="Ferramentas de importação e batimento DETRAF")
    sp = p.add_subparsers(dest="cmd", metavar="<comando>")

    run = sp.add_parser("run", help="Executa a importação e o batimento completo")
    run.add_argument("--config", action="store_true", help="Configurar variáveis (período/EOT/arquivo) durante a execução")
    run.set_defaults(func=cmd_run)

    cfg = sp.add_parser("config", help="Configura período, EOT e caminho do arquivo DETRAF")
    cfg.set_defaults(func=cmd_config)

    dbc = sp.add_parser("db-check", help="Valida a conexão com o banco (SELECT 1)")
    dbc.set_defaults(func=cmd_db_check)

    dbconf = sp.add_parser("db-config", help="Configura a conexão do banco (host, porta, user, password, database)")
    dbconf.set_defaults(func=cmd_db_config)

    return p

def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not hasattr(args, "func"):
        parser.print_help()
        return 2
    return args.func(args)

def app() -> int:
    return main()

if __name__ == "__main__":
    sys.exit(app())
