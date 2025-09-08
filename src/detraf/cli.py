#!/usr/bin/env python3
import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime
from calendar import monthrange

CONFIG_PATH = Path.home() / ".detraf_cli.json"
LAYOUT_YAML = "configs/detraf_layout.yaml"  # layout fixo, não configurável aqui

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
        os.environ.setdefault("DB_PASS", db.get("password", ""))
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
    # Fallback direto via PyMySQL se não houver helper do projeto
    import pymysql
    params = dict(
        host=os.getenv("DB_HOST","localhost"),
        port=int(os.getenv("DB_PORT","3306")),
        user=os.getenv("DB_USER","root"),
        password=os.getenv("DB_PASS",""),
        database=os.getenv("DB_NAME",""),
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    conn = pymysql.connect(**params)
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE detraf")
            cur.execute("TRUNCATE TABLE detraf_conferencia")
    finally:
        conn.close()

def db_select_1() -> bool:
    import pymysql
    try:
        params = dict(
            host=os.getenv("DB_HOST","localhost"),
            port=int(os.getenv("DB_PORT","3306")),
            user=os.getenv("DB_USER","root"),
            password=os.getenv("DB_PASS",""),
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
    print("\n╭────────────────────────────╮\n│ VALIDAÇÃO INICIAL DO BANCO │\n╰────────────────────────────╯")
    if not all(os.getenv(k) for k in ("DB_HOST","DB_USER","DB_NAME")) and not load_cfg().get("db"):
        err("Configuração de banco ausente. Rode: detraf db-config")
        return 1
    ensure_db_env_or_fail()
    if db_select_1():
        ok("Conexão OK (SELECT 1 -> 1)")
        return 0
    err("Falha na conexão ao banco.")
    return 1

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
    print("\n╭────────────────────────────╮\n│ VALIDAÇÃO INICIAL DO BANCO │\n╰────────────────────────────╯")
    if not db_select_1():
        err("Falha na conexão ao banco.")
        return 1
    ok("Conexão OK (SELECT 1 -> 1)")

    # 2) Coleta das variáveis: se --config, pergunta e salva; senão, usa cfg (sem perguntar)
    print("╭─────────────────╮\n│ COLETA DE DADOS │\n╰─────────────────╯")
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
    print("╭───────────────╮\n│ PROCESSAMENTO │\n╰───────────────╯")
    print("╭──────────────────────────────────╮\n│ CONTEXTO DE COBRANÇA (OPERADORA) │\n╰──────────────────────────────────╯")
    janela_ini, janela_fim = month_window_yyyymm(periodo)
    ok(f"janela_referencia_ini = {janela_ini}")
    ok(f"janela_referencia_fim = {janela_fim}")
    ok("Regra: chamadas do mês de referência serão consideradas (layout fixo YAML).")

    # 4) Preparação de banco (truncate)
    print("╭─────────────────────╮\n│ PREPARAÇÃO DE BANCO │\n╰─────────────────────╯")
    try:
        # se existir util do projeto, usa; senão, fallback
        try:
            from .processing import truncate_tables  # type: ignore
            truncate_tables()
        except Exception:
            truncate_tables_fallback()
        ok("Tabelas detraf e detraf_conferencia truncadas para novo processamento.")
        ok("Preparação concluída. Próxima etapa: importação do DETRAF (layout fixo).")
    except Exception as ex:
        err(f"Falha ao preparar o banco: {ex}")
        return 1

    # 5) Importação DETRAF
    print("╭─────────────────────────────────╮\n│ IMPORTAÇÃO DETRAF (LAYOUT FIXO) │\n╰─────────────────────────────────╯")
    ok("Janela aplicada. Iniciando importação do arquivo...")
    try:
        from .import_detraf import importar_arquivo_txt
    except Exception as ex:
        err(f"Falha ao carregar importação: {ex}")
        return 1

    try:
        resumo = importar_arquivo_txt(arquivo, periodo, eot, layout_path=LAYOUT_YAML)
        ok(f"Arquivo importado e persistido ({resumo.get('inseridos',0)} linhas).")
    except Exception as ex:
        err(f"Falha na importação do arquivo: {ex}")
        raise

    # 6) Próximas etapas (normalização/comparação) — delega ao projeto se existirem
    try:
        from .processing import processar_match  # type: ignore
        ok("Importação concluída. Prosseguindo com matching e classificação...")
        processar_match(periodo)
        ok("Processamento completo: detraf_conferencia preenchida.")
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
