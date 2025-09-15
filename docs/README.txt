detraf-batimento v2.0

Analisador automático do DETRAF com CDR, focado em simplicidade:
- Setup do banco
- Coleta de variáveis (período, EOT, arquivo)
- Execução da análise

Repositório
-----------
git clone https://github.com/KevynSilveira/AnaliseDetrafAvancado.git
cd AnaliseDetrafAvancado

Requisitos
----------
- Python 3.10+ recomendado
- MySQL ou MariaDB acessível (local ou remoto)
- Usuário de banco com permissões de leitura e escrita no schema dedicado
- Sistema com acesso de rede ao banco (execução remota possível)

Durante a configuração serão solicitados: host, porta (3306), usuário, senha e nome do banco.

Instalação (modo desenvolvimento)
---------------------------------
python3 -m venv .venv && source .venv/bin/activate
pip install -e .

Observação Debian/Ubuntu: se o venv falhar, instale python3-venv e recrie o ambiente.
sudo apt-get update && sudo apt-get install -y python3-venv
python3 -m venv .venv && source .venv/bin/activate

Configuração do Banco
---------------------
detraf db-config    # configura host, porta, user, senha, database
detraf db-check     # valida a conexão (SELECT 1)

Configuração do Processo (período/EOT/arquivo)
-----------------------------------------------
detraf config       # grava período YYYYMM, EOT (3 dígitos) e caminho do arquivo DETRAF

Execução da Análise
-------------------
./scripts/detraf run   # forma 1: launcher do repositório
detraf run             # forma 2: entrypoint instalado no venv
detraf run --config    # forma 3: coleta/atualiza variáveis durante a execução

Execução remota (sem instalar no servidor do cliente)
-----------------------------------------------------
- Garanta acesso de rede ao banco do cliente.
- Rode detraf db-config informando host/porta/usuário/senha do banco do cliente.
- Execute detraf run normalmente. Nada precisa ser instalado no servidor além do usuário de banco.

Saídas e logs
-------------
- Ao final, o CLI imprime no terminal o caminho completo do relatório gerado.
- Diretórios em tempo de execução: var/logs/ (logs), var/output/ (resultados), var/tmp/ (temporários).

Janela de cobrança por mês de referência
----------------------------------------
- Dado YYYYMM, calcula-se uma janela de 3 meses para a operadora:
  Início: primeiro dia de (mês - 2) às 00:00:00
  Fim: último dia do mês de referência às 23:59:59
- Exemplo: 202505 => 2025-03-01 00:00:00 até 2025-05-31 23:59:59.
- Classificação: Conferido (encontrado no CDR) e Perdido (não encontrado no CDR).
- Futuro: validação inversa (o que deveria ser cobrado e não foi).

Replicação sem instalação (opcional)
------------------------------------
- Se o repositório incluir scripts/ e vendor/:
  1) Gerar vendor/: ./scripts/build_vendor.sh
  2) Confirmar pacotes em vendor/ (pymysql, typer, rich, pyyaml, dotenv)
  3) No destino: ./scripts/detraf config && ./scripts/detraf run

Dicas rápidas
-------------
- Ativar venv: source .venv/bin/activate
- Desativar venv: deactivate
- .env opcional para host/porta/usuário/senha/database
