# Documentação das Tabelas do Batimento DETRAF

Este documento descreve a utilidade de cada objeto (tabelas e view) que o projeto cria/usa, além de indicar a existência de triggers. A menos que explicitado, não há triggers definidas por este projeto.

Observação: tabelas de produção externas como `cdr`, `numeros_portados` e `cadup` são apenas lidas — nada é alterado nelas.

## Tabelas Persistentes

- detraf_arquivo_batimento_avancado:
  - Finalidade: armazena o arquivo DETRAF importado (layout fixo) para servir de base ao batimento.
  - Principais colunas: `id` (PK), `eot`, `sequencial`, `assinante_a_numero`, `eot_de_a`, `cnl_de_a`, `area_local_de_a`, `data_da_chamada`, `hora_de_atendimento`, `assinante_b_numero`, `eot_de_b`, `cnl_de_b`, `area_local_de_b`, `data_hora` (indexada).
  - Triggers: não há.

- detraf_processado_batimento_avancado:
  - Finalidade: resultado do batimento por linha do DETRAF, após matching (±5min, RN=1) ou marcação como "Perdido".
  - Principais colunas: `id` (PK), `detraf_id` (FK lógico para detraf_arquivo_batimento_avancado.id), `cdr_id` (FK lógico para cdr.id, pode ser NULL), `status` ("Conferência"|"Erro"|"Perdido"), `observacao` (texto explicativo), `created_at`. Índices: `idx_detraf_id`, `idx_cdr_id`.
  - Preenchimento: pela rotina de matching durante o `detraf run`.
  - Triggers: não há.

- detraf_context_batimento_avancado:
  - Finalidade: registra o período de referência da execução (janela do mês que define RECUPERAÇÃO DE CONTA).
  - Colunas: `id`, `periodo` (YYYYMM), `ref_ini`, `ref_fim`, `created_at`.
  - Ciclo de vida: truncada e gravada a cada execução (1 linha por run).
  - Triggers: não há.

- codigo_erro_batimento_avancado:
  - Finalidade: catálogo de códigos/descrições de erro usados nos relatórios/view.
  - Colunas: `codigo` (PK), `descricao`, `ativo`, `created_at`.
  - Popularização: inserções idempotentes (INSERT IGNORE) no setup.
  - Triggers: não há.

- cdr_batimento_avancado (opcional, não exigida pela view):
  - Finalidade: estrutura base para eventual persistência de CDR normalizado. Atualmente, o pipeline utiliza uma tabela TEMPORARY com este nome durante o matching e a view final consulta diretamente a tabela `cdr`.
  - Colunas: `id`, `calldate`, `src`, `dst`, `EOT_A`, `EOT_B`, `duration`, `billsec`, `sentido`, `disposition`.
  - Triggers: não há.

## Objetos Temporários (apenas durante o run)

- tmp_detraf_<runid> (TEMPORARY):
  - Finalidade: versão normalizada do DETRAF para matching (números apenas com dígitos, regra de corte 12/13 dígitos retirando 2 à esquerda).
  - Colunas típicas: `id`, `data_hora`, `eot_de_a`, `eot_de_b`, `a_num`, `b_num`.
  - Triggers: não há (temporária de sessão).

- cdr_batimento_avancado (TEMPORARY durante o matching):
  - Finalidade: CDR normalizado/selecionado na janela do DETRAF para acelerar o matching.
  - Colunas: `id`, `calldate`, `src`, `dst`, `EOT_A`, `EOT_B`, `duration`, `billsec`, `sentido`, `disposition`.
  - Triggers: não há (temporária de sessão).

- tmp_conf_<runid> (TEMPORARY):
  - Finalidade: candidatos de matching com `diff_sec` e RN=1 por `detraf_id`.
  - Colunas: variam por execução (inclui `detraf_id`, `cdr_id`, `diff_sec`, `disposition`, etc.).
  - Triggers: não há (temporária de sessão).

## View Persistente

- detraf_batimento_avancado_vw:
  - Finalidade: visão consolidada para relatório/CSV. Classifica STATUS (Conferência, Erro, Perdido), calcula `diferenca_tempo` em mm:ss, expõe dados do batimento (origem/destino/EOTs), dados do CDR (id/EOTs), `codigo_erro` e `observacao` (motivo detalhado).
  - Fontes: `detraf_processado_batimento_avancado` (linhas processadas), `detraf_arquivo_batimento_avancado` (DET/REF), `cdr` (EOTs do CDR).
  - Ordenação padrão embutida: STATUS (Conferência → Erro → Perdido), depois `Data_hora_batimento`, depois `codigo_erro`.
  - Triggers: não se aplicam (view não suporta triggers).

## Tabelas Externas (somente leitura)

- cdr: tabela de chamadas (fonte do CDR); consultada, não alterada.
- numeros_portados: base de portabilidade; consultada quando necessário.
- cadup: cadastro de planos (CN/prefixo/MCDU); consultada quando necessário.

## Triggers

- O projeto não cria triggers em nenhuma das tabelas. Se o ambiente possuir triggers próprias, elas não são gerenciadas por este repositório.

## Códigos de Erro (catálogo)

Os códigos abaixo são cadastrados em `codigo_erro_batimento_avancado` e usados pela view/relatórios.

- 1: Cobranca indevida - chamada com disposition diferente de atendida.
- 2: EOT de B do batimento diferente do EOT de B do CDR.
- 3: EOT de A do batimento diferente do EOT de A do CDR.
- 4: Chamada do batimento nao encontrado no CDR. (Perdido)
- 5: EOT de A e de B do batimento nao bate com o CDR.

Observações
- Para chamadas não atendidas (disposition ≠ ANSWERED), não é feita validação de EOT; o status é "Erro" com código 1.
- "Recuperação de conta" não é um código de erro: é um marcador (aparece na coluna `observacao`) indicando que o registro está fora da janela do mês de referência.
