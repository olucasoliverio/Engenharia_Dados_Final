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

## Popular o banco com os dados simulados

Os dados simulados estão em `dataset/arquivos_csv/` (gerados pelos scripts
`dataset/scripts_py/gerar_*.py`). O script `carregar_mongo.py` lê esses CSVs,
converte os tipos (datas viram `ISODate`, números viram int/float, campos vazios
viram `null`), cria as 10 coleções com os validadores `$jsonSchema` de
`mongodb/schemas/` e os índices (chave primária, chaves estrangeiras e
`updated_at`).

```bash
# com o container de pé (docker compose up -d) e o .env configurado:
python3 -m venv .venv
source .venv/bin/activate
pip install -r dataset/scripts_py/requirements.txt

python dataset/scripts_py/carregar_mongo.py
```

Opções úteis:

```bash
python dataset/scripts_py/carregar_mongo.py --only pedidos   # uma coleção só
python dataset/scripts_py/carregar_mongo.py --no-validator   # sem $jsonSchema
python dataset/scripts_py/carregar_mongo.py --uri "<MONGO_URI>" --db ecommerce
```

A conexão é resolvida por `MONGO_URI` / `MONGO_DB` (CLI > variável de ambiente >
`.env` > default local). O script é idempotente: recria cada coleção a cada
execução.

## MongoDB compartilhado (Atlas)

Para o time acessar a mesma origem, a base também roda em um cluster gratuito no
MongoDB Atlas. O mesmo `carregar_mongo.py` é usado — muda apenas o `MONGO_URI`
no `.env` (conexão `mongodb+srv://`, que requer o `dnspython` do
`requirements.txt`). Passo a passo completo em
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

Testes unitários da lógica de ingestão:

```bash
PYTHONPYCACHEPREFIX=/tmp/engenharia_dados_pycache \
  python3 -m unittest discover -s tests -v
```
