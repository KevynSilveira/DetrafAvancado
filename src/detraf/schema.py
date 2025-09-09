"""Utilitário para recriar tabelas necessárias ao DETRAF.

Uso:
    python -m detraf.schema

O script elimina e recria as tabelas ``detraf`` e ``detraf_conferencia``
com campos compatíveis com o pipeline de importação e batimento.
"""

from __future__ import annotations
import pymysql
from .db import get_connection
from .log import info, ok

CREATE_DETRAF = """
CREATE TABLE detraf (
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

CREATE_CONFERENCIA = """
CREATE TABLE detraf_conferencia (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    detraf_id BIGINT NOT NULL,
    cdr_id BIGINT NULL,
    status VARCHAR(20) NOT NULL,
    observacao VARCHAR(255) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_detraf_id (detraf_id),
    INDEX idx_cdr_id (cdr_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def reset_schema() -> None:
    """Dropa e recria as tabelas ``detraf`` e ``detraf_conferencia``."""
    with get_connection() as conn:  # autocommit=True no get_connection
        cur = conn.cursor()
        info("Removendo tabelas antigas se existirem...")
        cur.execute("DROP TABLE IF EXISTS detraf_conferencia")
        cur.execute("DROP TABLE IF EXISTS detraf")
        info("Criando tabela detraf...")
        cur.execute(CREATE_DETRAF)
        info("Criando tabela detraf_conferencia...")
        cur.execute(CREATE_CONFERENCIA)
        ok("Tabelas detraf e detraf_conferencia recriadas.")

if __name__ == "__main__":
    reset_schema()
