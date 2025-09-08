
from __future__ import annotations
from typing import Optional
import pymysql
from .env import load_env
from .log import ok, err

def is_db_configured() -> bool:
    env = load_env()
    req = ["DB_HOST", "DB_PORT", "DB_USER", "DB_PASSWORD", "DB_NAME"]
    return all(env.get(k) for k in req)

def get_conn():
    """Connection factory expected by import_detraf_fw (name: get_conn)."""
    env = load_env()
    conn = pymysql.connect(
        host=env.get("DB_HOST", "localhost"),
        port=int(env.get("DB_PORT", "3306")),
        user=env.get("DB_USER", ""),
        password=env.get("DB_PASSWORD", ""),
        database=env.get("DB_NAME", ""),
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    return conn

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
