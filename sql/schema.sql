-- Script manual para recriar as tabelas utilizadas pelo DETRAF
-- Não executado automaticamente; use conforme necessário.

DROP TABLE IF EXISTS detraf_conferencia;
DROP TABLE IF EXISTS detraf;

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

