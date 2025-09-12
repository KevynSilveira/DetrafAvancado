# detraf-batimento v2.0

Analisador automático do DETRAF com CDR focado em simplicidade: **setup do banco**, **coleta de dados (período, EOT, arquivo)** e **execução**.

## Instalação (modo desenvolvimento)
```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
```

## Configuração do Banco
```bash
# configura o banco (host, porta, user, senha, database)
detraf db-config

# valida a conexão (SELECT 1)
detraf db-check
```

## Configuração do Processo (período/EOT/arquivo)
```bash
# grava período (YYYYMM), EOT (3 dígitos) e caminho do arquivo DETRAF
detraf config
```

## Execução da Análise
```bash
# usa as variáveis já salvas (período/EOT/arquivo)
detraf run

# ou peça para coletar/atualizar as variáveis durante a execução:
detraf run --config
```

> **Nota:** Esta versão foca na **etapa inicial** (setup do banco + inicializador de dados). A importação do arquivo e etapas seguintes estão estruturadas para evolução rápida.


## Janela de cobrança por mês de referência
Ao informar `YYYYMM` (ex.: `202505`), o sistema calcula uma **janela de 3 meses** para o lado da operadora:
- Início: primeiro dia de (mês-2) às 00:00:00
- Fim: último dia do mês de referência às 23:59:59

Ex.: `202505` => janela `202503-01 00:00:00` até `2025-05-31 23:59:59`.
Essa janela é usada para classificar registros do arquivo da operadora em **conferido** (encontrado no CDR) ou **perdido** (não encontrado), e servirá para etapas futuras onde verificaremos o inverso (o que deveria ser cobrado e não foi).
