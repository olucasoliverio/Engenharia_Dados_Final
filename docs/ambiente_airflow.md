# Ambiente Airflow local

Implementação da **Issue #19**, responsável por subir o Apache Airflow via
Docker para orquestrar as DAGs do projeto.

## Objetivo

Disponibilizar um ambiente local reproduzível com Airflow, MongoDB e MinIO no
mesmo `docker-compose.yml`. O stack permite desenvolver, importar e testar a DAG
`mongodb_to_landing` sem instalar Airflow diretamente na máquina.

A configuração foi baseada no guia oficial "Running Airflow in Docker", usando
Airflow 3.2.2 e uma imagem customizada para instalar os providers necessários.

## Serviços

| Serviço | Função | Porta |
|---|---|---|
| `airflow-apiserver` | Interface web e API do Airflow | `8080` |
| `airflow-scheduler` | Agendamento e disparo das tasks | - |
| `airflow-dag-processor` | Processamento e importação das DAGs | - |
| `airflow-triggerer` | Execução de tarefas deferrable | - |
| `airflow-postgres` | Metadados do Airflow | - |
| `mongodb` | Origem local dos dados | `27017` |
| `minio` | Object storage compatível com S3 | `9000`, `9001` |

O Airflow usa `LocalExecutor`, suficiente para desenvolvimento local e mais
leve que um cluster com Celery/Redis.

## Arquivos da entrega

| Arquivo | Responsabilidade |
|---|---|
| `docker-compose.yml` | Stack local com MongoDB, MinIO, Postgres e Airflow |
| `Dockerfile.airflow` | Imagem customizada baseada em `apache/airflow` |
| `requirements-airflow.txt` | Providers de MongoDB e Amazon/S3 usados pela DAG |
| `.env.example` | Variáveis locais, usuário inicial e Connections do Airflow |
| `.gitignore` | Ignora logs e configuração local gerada pelo Airflow |

## Subir o ambiente

Crie o arquivo local de variáveis:

```bash
cp .env.example .env
```

Suba o stack completo:

```bash
docker compose up -d --build \
  mongodb \
  minio \
  airflow-apiserver \
  airflow-scheduler \
  airflow-dag-processor \
  airflow-triggerer
```

O serviço `airflow-init` executa automaticamente como dependência, aplica as
migrações do banco de metadados e cria o usuário inicial.

Verifique os containers:

```bash
docker compose ps
```

## Acessar a interface

Interface do Airflow:

```text
http://localhost:8080
```

Credenciais padrão do `.env.example`:

| Campo | Valor |
|---|---|
| Usuário | `airflow` |
| Senha | `airflow` |

Troque `_AIRFLOW_WWW_USER_PASSWORD` no seu `.env` se o ambiente ficar acessível
fora da máquina local.

## Connections

As Connections são criadas por variáveis de ambiente, sem salvar credenciais no
repositório.

| Connection Id | Variável | Padrão local |
|---|---|---|
| `mongodb_atlas` | `AIRFLOW_CONN_MONGODB_ATLAS` | MongoDB Docker em `mongodb:27017` |
| `minio_s3` | `AIRFLOW_CONN_MINIO_S3` | MinIO Docker em `minio:9000` |

Para usar Atlas, sobrescreva `AIRFLOW_CONN_MONGODB_ATLAS` no seu `.env`.
Exemplo de formato:

```bash
AIRFLOW_CONN_MONGODB_ATLAS=mongo://USUARIO:SENHA@cluster.mongodb.net/ecommerce?srv=true&ssl=true&authSource=admin&retryWrites=true&w=majority
```

## Validar DAGs

Depois que os containers estiverem saudáveis, valide a importação das DAGs:

```bash
docker compose exec airflow-apiserver airflow dags list-import-errors
docker compose exec airflow-apiserver airflow dags list
```

Para testar a DAG `mongodb_to_landing`, garanta antes que:

1. o MongoDB esteja populado;
2. o bucket e os prefixos da Landing existam no MinIO;
3. as Connections estejam apontando para a origem desejada.

Com tudo preparado:

```bash
docker compose exec airflow-apiserver airflow dags test mongodb_to_landing 2026-06-13
```

## Parar ou limpar

Parar mantendo volumes:

```bash
docker compose down
```

Parar removendo dados locais do MongoDB, MinIO e metadados do Airflow:

```bash
docker compose down -v
```
