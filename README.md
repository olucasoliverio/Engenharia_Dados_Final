# Engenharia de Dados — Projeto Final

Pipeline de dados de um e-commerce fictício (origem MongoDB → Data Lake medalhão
→ dashboard). Documentação completa publicada via MkDocs.

## Integrantes

- Guilherme Madalena
- Gustavo Felisbino
- Lucas Gaspar
- Lucas Oliverio
- Luiz Barros
- Tiago Mazzuco

## Ambiente MongoDB local (origem)

A origem dos dados é um MongoDB rodando em Docker. O modelo das coleções está
documentado em [`docs/modelo_mongodb.md`](docs/modelo_mongodb.md).

### Pré-requisitos

- Docker + Docker Compose

### Subir o banco

```bash
# 1. Crie seu .env a partir do exemplo (não é commitado)
cp .env.example .env

# 2. Suba o container
docker compose up -d

# 3. Verifique a saúde do container (aguarde STATUS = healthy)
docker compose ps
```

O serviço sobe em `localhost:27017` com o banco `ecommerce`. As credenciais e a
string de conexão ficam no `.env` (veja `.env.example`).

### Parar / limpar

```bash
docker compose down        # para o container (mantém os dados no volume)
docker compose down -v     # para e APAGA os dados (remove o volume)
```

## Gerar e popular o banco com os dados simulados

Os CSVs em `dataset/arquivos_csv/` **não são versionados** (reproduzíveis e
pesam ~15 MB). O `carregar_mongo.py` **gera os CSVs automaticamente** quando
estão ausentes (chamando `gerar_dados.py`) e em seguida carrega no Mongo:
converte os tipos (datas viram `ISODate`, números viram int/float, campos vazios
viram `null`), cria as 10 coleções com os validadores `$jsonSchema` de
`mongodb/schemas/` e os índices (chave primária, chaves estrangeiras e
`updated_at`).

```bash
# com o container de pé (docker compose up -d) e o .env configurado:
python3 -m venv .venv
source .venv/bin/activate
pip install ".[dataset]"

# gera os CSVs (se faltarem) e popula o Mongo num passo só:
python dataset/scripts_py/carregar_mongo.py
```

Os geradores usam sementes fixas, então a saída é determinística. Para só
(re)gerar os CSVs sem carregar: `python dataset/scripts_py/gerar_dados.py`.

Opções úteis:

```bash
python dataset/scripts_py/carregar_mongo.py --only pedidos   # uma coleção só
python dataset/scripts_py/carregar_mongo.py --no-validator   # sem $jsonSchema
python dataset/scripts_py/carregar_mongo.py --no-gerar       # nao gera CSV ausente
python dataset/scripts_py/carregar_mongo.py --uri "<MONGO_URI>" --db ecommerce
```

A conexão é resolvida por `MONGO_URI` / `MONGO_DB` (CLI > variável de ambiente >
`.env` > default local). O script é idempotente: recria cada coleção a cada
execução.

## MongoDB compartilhado (Atlas)

Para o time acessar a mesma origem, a base também roda em um cluster gratuito no
MongoDB Atlas. O mesmo `carregar_mongo.py` é usado — muda apenas o `MONGO_URI`
no `.env` (conexão `mongodb+srv://`, que requer o `dnspython` do grupo
`[dataset]` do `pyproject.toml`). Passo a passo completo em
[`docs/mongodb_atlas.md`](docs/mongodb_atlas.md).

## DAG MongoDB Atlas → Landing

A DAG [`mongodb_to_landing`](dags/mongodb_to_landing.py) extrai as dez coleções
do MongoDB Atlas para o object storage configurado no Airflow. A primeira
execução realiza uma carga completa; as seguintes usam checkpoints por coleção
baseados em `updated_at`.

Os documentos são gravados como MongoDB Extended JSON Lines, sem enriquecimento
ou alteração dos dados da origem:

```text
landing/ecommerce/<colecao>/extraction_date=AAAA-MM-DD/
  run_id=<airflow_run_id>/part-00000.json
```

Configuração das Connections, variáveis, execução e evidências estão
documentadas em
[`docs/dag_mongodb_landing.md`](docs/dag_mongodb_landing.md).

## Ambiente Airflow local

O Airflow roda via Docker Compose junto com MongoDB, MinIO e Postgres. O stack
usa uma imagem customizada baseada em `apache/airflow` para instalar os
providers de MongoDB e Amazon/S3 usados pela DAG.

```bash
cp .env.example .env

docker compose up -d --build \
  mongodb \
  minio \
  airflow-apiserver \
  airflow-scheduler \
  airflow-dag-processor \
  airflow-triggerer
```

Interface: <http://localhost:8080>

Credenciais locais padrão:

```text
usuario: airflow
senha: airflow
```

Valide a importação das DAGs:

```bash
docker compose exec airflow-apiserver airflow dags list-import-errors
docker compose exec airflow-apiserver airflow dags list
```

Detalhes de serviços, Connections e comandos de parada estão em
[`docs/ambiente_airflow.md`](docs/ambiente_airflow.md).

Testes unitários da lógica de ingestão:

```bash
PYTHONPYCACHEPREFIX=/tmp/engenharia_dados_pycache \
  python3 -m unittest discover -s tests -v
```

## Estrutura da camada Landing

O MinIO local fornece o object storage do Data Lake:

```bash
docker compose up -d minio
docker compose ps minio
```

- API S3: <http://localhost:9000>
- Console: <http://localhost:9001>

Depois de configurar o `.env`, crie ou valide o bucket e os prefixos da Landing:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install ".[infra]"

set -a
source .env
set +a

python scripts/criar_estrutura_landing.py
python scripts/criar_estrutura_landing.py --validate-only
```

O contrato versionado está em
[`config/landing_structure.json`](config/landing_structure.json). Detalhes em
[`docs/estrutura_landing.md`](docs/estrutura_landing.md).

## Estrutura da camada Bronze

A camada Bronze reserva os prefixos das dez tabelas Delta e registra o contrato
esperado no MinIO ou Amazon S3:

```bash
python scripts/criar_estrutura_bronze.py
python scripts/criar_estrutura_bronze.py --validate-only
```

Estrutura preparada:

```text
bronze/ecommerce/<tabela>/_READY
bronze/_control/_structure.json
```

O `_delta_log` e os arquivos Parquet serão criados pela primeira gravação da
DAG Landing → Bronze. O contrato está em
[`config/bronze_structure.json`](config/bronze_structure.json), com detalhes em
[`docs/estrutura_bronze.md`](docs/estrutura_bronze.md).

## DAG Landing → Bronze

A DAG `landing_to_bronze` submete um job PySpark que converte os JSONs da
Landing para dez tabelas Delta:

```text
landing/ecommerce/<colecao>/*.json
  → bronze/ecommerce/<colecao>/_delta_log/
  → bronze/ecommerce/<colecao>/ingestion_date=AAAA-MM-DD/*.parquet
```

O processamento é idempotente por arquivo de origem e gera um manifesto de
auditoria por execução. Configuração completa em
[`docs/dag_landing_bronze.md`](docs/dag_landing_bronze.md).

## Estrutura da camada Silver

A camada Silver reserva os prefixos das dez tabelas limpas e padronizadas:

```bash
python scripts/criar_estrutura_silver.py
python scripts/criar_estrutura_silver.py --validate-only
```

Estrutura preparada:

```text
silver/ecommerce/<tabela>/_READY
silver/_control/_structure.json
```

A DAG Bronze → Silver cria os arquivos Delta e aplica as regras de
qualidade. O contrato está em
[`config/silver_structure.json`](config/silver_structure.json), com detalhes em
[`docs/estrutura_silver.md`](docs/estrutura_silver.md).

## DAG Bronze → Silver

A DAG `bronze_to_silver` transforma o MongoDB Extended JSON preservado na
Bronze em dez tabelas Delta tipadas e validadas:

```text
bronze/ecommerce/<tabela>/
  → deduplicação, limpeza e validação
  → silver/ecommerce/<tabela>/_delta_log/
```

O job mantém o registro mais recente por chave primária, valida domínios e
relacionamentos e executa `MERGE` incremental. Cada execução gera um manifesto
com inserções, atualizações, rejeições e duplicatas removidas. Configuração
completa em
[`docs/dag_bronze_silver.md`](docs/dag_bronze_silver.md).

## Estrutura da camada Gold

A camada Gold reserva as dimensões e fatos do modelo analítico:

```bash
python scripts/criar_estrutura_gold.py
python scripts/criar_estrutura_gold.py --validate-only
```

Estrutura preparada:

```text
gold/ecommerce/<dimensao-ou-fato>/_READY
gold/_control/_structure.json
```

O modelo possui quatro dimensões e quatro fatos para análises de vendas,
pagamentos, entregas e avaliações. O contrato está em
[`config/gold_structure.json`](config/gold_structure.json), com detalhes em
[`docs/estrutura_gold.md`](docs/estrutura_gold.md).

## DAG Silver → Gold

A DAG `silver_to_gold` materializa o modelo dimensional analítico a partir das
dez tabelas Silver:

```text
silver/ecommerce/<tabela>/
  → dimensões de tempo, cliente, produto e cupom
  → fatos de vendas, pagamentos, entregas e avaliações
  → gold/ecommerce/<dimensao-ou-fato>/_delta_log/
```

O job enriquece produtos com categoria e fornecedor, calcula medidas de
receita, pagamentos aprovados, prazo e atraso de entregas e classificação das
avaliações. As tabelas fato são particionadas por ano e sincronizadas por
`MERGE`, sem duplicar dados em novas execuções. Configuração completa em
[`docs/dag_silver_gold.md`](docs/dag_silver_gold.md).

## Documentação (MkDocs)

As páginas-fonte ficam em `docs/` e o `mkdocs.yml` na raiz. O diretório `site/`
(saída do build) **não é versionado** — é gerado localmente e publicado no
GitHub Pages.

```bash
pip install mkdocs
mkdocs serve      # pré-visualização em http://127.0.0.1:8000
mkdocs build      # gera site/ (local, ignorado pelo git)
mkdocs gh-deploy  # publica no GitHub Pages
```

## Documentação Interativa (Jupyter Notebooks)

Além da documentação publicada via MkDocs, o projeto disponibiliza uma coleção de notebooks Jupyter para facilitar o estudo e compreensão da solução de forma guiada e interativa.

Os notebooks apresentam a arquitetura, regras de negócio, fluxo de dados, banco de dados, integrações, infraestrutura e execução operacional do pipeline em uma sequência estruturada, servindo como material de onboarding e transferência de conhecimento.

### Estrutura

```text
notebooks/
├── 00_indice_documentacao.ipynb
├── 01_visao_geral_projeto.ipynb
├── 02_estrutura_repositorio.ipynb
├── 03_arquitetura_detalhada.ipynb
├── 04_processos_negocio.ipynb
├── 05_fluxo_dados.ipynb
├── 06_banco_dados.ipynb
├── 07_interfaces_apis.ipynb
├── 08_infraestrutura.ipynb
└── 09_execucao_pipeline.ipynb
```

### Executando localmente

```bash
python3 -m venv .venv
source .venv/bin/activate

pip install "[.notebooks]"

python run jupyter lab
```

Após abrir o Jupyter, navegue até o diretório `notebooks/` e execute os arquivos na ordem numérica.
