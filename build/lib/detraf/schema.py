"""Utilitário para criar/atualizar as tabelas do batimento avançado.

Uso:
    python -m detraf.schema

Este script gerencia apenas objetos do batimento avançado e NÃO altera
as tabelas de produção `cdr`, `numeros_portados` ou `cadup`.
"""

from __future__ import annotations
from .db import get_connection
from .log import info, ok

CREATE_DETRAF_ARQUIVO = """
CREATE TABLE detraf_arquivo_batimento_avancado (
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

CREATE_DETRAF_PROCESSADO = """
CREATE TABLE detraf_processado_batimento_avancado (
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

CREATE_CODIGO_ERRO = """
CREATE TABLE IF NOT EXISTS codigo_erro_batimento_avancado (
    codigo INT PRIMARY KEY,
    descricao VARCHAR(255) NOT NULL,
    ativo TINYINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

SEED_CODIGO_ERRO = """
INSERT IGNORE INTO codigo_erro_batimento_avancado (codigo, descricao, ativo) VALUES
  (1,'Cobranca indevida - chamada com disposition diferente de atendida.',1),
  (2,'EOT de B do batimento diferente do EOT de B do CDR.',1),
  (3,'EOT de A do batimento diferente do EOT de A do CDR.',1),
  (4,'Chamada do batimento nao encontrado no CDR.',1),
  (5,'EOT de A e de B do batimento nao bate com o CDR.',1);
"""

def reset_schema_avancado() -> None:
    """Dropa e recria as tabelas avançadas do batimento.

    Não altera `cdr`, `numeros_portados` ou `cadup`.
    """
    with get_connection() as conn:  # autocommit=True no get_connection
        cur = conn.cursor()
        info("Removendo tabelas do batimento avançado se existirem...")
        cur.execute("DROP TABLE IF EXISTS detraf_processado_batimento_avancado")
        cur.execute("DROP TABLE IF EXISTS detraf_arquivo_batimento_avancado")
        info("Criando tabela detraf_arquivo_batimento_avancado...")
        cur.execute(CREATE_DETRAF_ARQUIVO)
        info("Criando tabela detraf_processado_batimento_avancado...")
        cur.execute(CREATE_DETRAF_PROCESSADO)
        info("Criando/atualizando tabela de códigos de erro...")
        cur.execute(CREATE_CODIGO_ERRO)
        cur.execute(SEED_CODIGO_ERRO)
        ok("Tabelas do batimento avançado recriadas e catálogo populado.")

if __name__ == "__main__":
    reset_schema_avancado()
