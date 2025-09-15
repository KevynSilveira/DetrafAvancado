Q# detraf-batimento v2.0

Analisador automático do DETRAF com CDR, focado em simplicidade:
- Setup do banco
- Coleta de variáveis (período, EOT, arquivo)
- Execução da análise

## Repositório

```bash
git clone https://github.com/KevynSilveira/AnaliseDetrafAvancado.git
cd AnaliseDetrafAvancado
```

## Requisitos

- Python 3.10+ recomendado
- MySQL ou MariaDB acessível (local ou remoto)
- Usuário de banco com permissões de leitura e escrita no schema dedicado
- Sistema com acesso de rede ao banco (para execução remota, a aplicação pode rodar na sua máquina apontando para o banco do cliente)

Durante a configuração serão solicitados:
- Host do banco (IP ou hostname)
- Porta (padrão 3306)
- Usuário e senha
- Nome do banco de dados

## Instalação (modo desenvolvimento)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -e .
```
Observação (Debian/Ubuntu): se o comando de venv falhar, instale o pacote do sistema e recrie o ambiente virtual:
```bash
sudo apt-get update && sudo apt-get install -y python3-venv
python3 -m venv .venv && source .venv/bin/activate
```

## Configuração do Banco

```bash
# configura parâmetros de conexão (host, porta, user, senha, database)
detraf db-config

# valida conexão com o banco (SELECT 1)
detraf db-check
```

## Configuração do Processo (período/EOT/arquivo)

```bash
# grava período (YYYYMM), EOT (3 dígitos) e caminho do arquivo DETRAF
detraf config
```

## Execução da Análise

```bash
# forma 1: sem instalar nada no sistema, usando o launcher do repositório
./scripts/detraf run

# forma 2: executando pelo entrypoint instalado no ambiente virtual
detraf run

# forma 3: solicitando coleta/atualização das variáveis durante a execução
detraf run --config
```

### Execução remota (sem instalar nada no servidor do cliente)
A aplicação pode rodar na sua máquina e se conectar ao banco do cliente via rede. Para isso:
1) Garanta que o banco do cliente esteja acessível (host/porta liberados).
2) Rode `detraf db-config` informando o host/porta/usuário/senha do banco no cliente.
3) Execute `detraf run` normalmente. Nada precisa ser instalado no servidor além do usuário de banco.

## Saídas e logs

- Ao final, o CLI imprime no terminal o **caminho completo** do arquivo/relatório gerado.
- Por padrão, a estrutura de execução cria diretórios em tempo de execução:
  - `var/logs/` para arquivos de log (ex.: `var/logs/run_<run_id>.log`)
  - `var/output/` para resultados (ex.: relatórios CSV)
  - `var/tmp/` para temporários
- O caminho exato dos relatórios é exibido ao término da execução.

## Janela de cobrança por mês de referência

Ao informar `YYYYMM` (por exemplo, `202505`), o sistema calcula uma **janela de 3 meses** para o lado da operadora:
- Início: primeiro dia de (mês - 2) às 00:00:00
- Fim: último dia do mês de referência às 23:59:59

Exemplo: `202505` => janela `2025-03-01 00:00:00` até `2025-05-31 23:59:59`.

Essa janela é usada para classificar registros do arquivo da operadora em:
- **Conferido**: encontrado no CDR dentro das regras de matching
- **Perdido**: presente no arquivo da operadora, mas não encontrado no CDR

Etapas futuras incluirão a validação inversa (chamadas do CDR que deveriam ser cobradas no mês de referência e não apareceram no arquivo da operadora).

## Replicação sem instalação (opcional)

Caso o repositório inclua o launcher em `scripts/` e a pasta `vendor/`:
1) Em uma máquina com internet, gere `vendor/` com dependências:
```bash
./scripts/build_vendor.sh
```
2) Confirme que `vendor/` contém os pacotes necessários (pymysql, typer, rich, pyyaml, dotenv).
3) No servidor de destino, apenas clone e rode:
```bash
./scripts/detraf config   # configura banco e período/EOT/arquivo
./scripts/detraf run
```
O launcher detecta `vendor/` e executa `python -m detraf` sem instalar nada no sistema.

## Dicas rápidas

- Ativar venv: `source .venv/bin/activate`
- Desativar venv: `deactivate`
- Se necessário, configure variáveis em `.env` (opcional) para host/porta/usuário/senha/database.
