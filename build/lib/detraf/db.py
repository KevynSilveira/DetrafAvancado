
from __future__ import annotations
import pymysql
from .env import load_env
from .log import ok, err

def is_db_configured() -> bool:
    env = load_env()
    req = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    return all(env.get(k) for k in req)

def get_conn_params() -> dict:
    """Retorna kwargs padrão para ``pymysql.connect``.

    Usado por módulos que precisam de uma conexão explícita ou para
    obter os parâmetros e ajustar ``autocommit`` ou outras opções.
    """
    env = load_env()
    return {
        "host": env.get("DB_HOST", "localhost"),
        "port": int(env.get("DB_PORT", "3306")),
        "user": env.get("DB_USER", ""),
        "password": env.get("DB_PASSWORD", ""),
        "database": env.get("DB_NAME", ""),
        "autocommit": True,
        "cursorclass": pymysql.cursors.DictCursor,
    }


def get_connection():
    """Conveniência para ``pymysql.connect`` usando ``get_conn_params``."""
    return pymysql.connect(**get_conn_params())


def get_conn():
    """Factory compatível com ``import_detraf_fw`` (nome legado)."""
    return get_connection()

def test_connection() -> bool:
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT 1")
            row = cur.fetchone()
            ok(f"Conexão OK (SELECT 1 -> {list(row.values())[0]})")
        return True
    except Exception as ex:
        err(f"Falha na conexão: {ex}")
        return False
