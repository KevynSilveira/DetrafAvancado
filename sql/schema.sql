-- =============================================
-- Tabelas do Batimento Avançado (usadas no run)
-- =============================================

-- Tabela com arquivo DETRAF importado (layout fixo)
DROP TABLE IF EXISTS detraf_arquivo_batimento_avancado;
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

-- Resultado do processamento (match/perdidas/erros)
DROP TABLE IF EXISTS detraf_processado_batimento_avancado;
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

-- Tabela de códigos de erro (catálogo)
DROP TABLE IF EXISTS codigo_erro_batimento_avancado;
CREATE TABLE codigo_erro_batimento_avancado (
    codigo INT PRIMARY KEY,
    descricao VARCHAR(255) NOT NULL,
    ativo TINYINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed básico de códigos (idempotente)
INSERT IGNORE INTO codigo_erro_batimento_avancado (codigo, descricao, ativo) VALUES
  (1,'Cobranca indevida - chamada com disposition diferente de atendida.',1),
  (2,'EOT de B do batimento diferente do EOT de B do CDR.',1),
  (3,'EOT de A do batimento diferente do EOT de A do CDR.',1),
  (4,'Chamada do batimento nao encontrado no CDR.',1),
  (5,'EOT de A e de B do batimento nao bate com o CDR.',1);

-- Contexto do período de referência do batimento (1 linha por execução)
CREATE TABLE IF NOT EXISTS detraf_context_batimento_avancado (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    periodo CHAR(6) NOT NULL,
    ref_ini DATETIME NOT NULL,
    ref_fim DATETIME NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabela de CDR normalizado para batimento (persistida)
-- Obs.: o pipeline recria/trunca esta tabela; aqui deixamos o DDL base
CREATE TABLE IF NOT EXISTS cdr_batimento_avancado (
    id BIGINT,
    calldate DATETIME,
    src VARCHAR(32),
    dst VARCHAR(32),
    EOT_A VARCHAR(32),
    EOT_B VARCHAR(32),
    duration INT,
    billsec INT,
    sentido VARCHAR(16),
    disposition VARCHAR(32)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- View consolidada para consulta (depende das tabelas acima e da `cdr`)
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
ORDER BY FIELD(STATUS, 'Conferência','Erro','Perdido'), d.data_hora, codigo_erro;
