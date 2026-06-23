---
tags:
  - airflow
  - docker
  - ambiente
---

# Ambiente Airflow local

Implementação da **Issue #19**, responsável por subir o Apache Airflow via
Docker para **orquestrar as DAGs** do projeto (da Landing até a Gold). O mesmo
`docker-compose.yml` também sobe as dependências de dados — MongoDB e MinIO —,
de modo que um único comando entrega o stack inteiro pronto para executar o
pipeline Medalhão.

A configuração foi baseada no guia oficial *"Running Airflow in Docker"*, usando
**Airflow 3.2.2** e uma imagem customizada que instala os providers necessários
(MongoDB e Amazon/S3).

!!! abstract "Em resumo"
    - **O que sobe?** MongoDB (origem), MinIO (data lake S3), Postgres
      (metadados) e os componentes do Airflow (apiserver, scheduler,
      dag-processor, triggerer), além de um `airflow-init` que prepara o banco.
    - **Por que `LocalExecutor`?** Roda as tasks em processos locais, sem
      precisar de Celery/Redis — mais leve e suficiente para desenvolvimento.
    - **Como acesso?** Interface web em `http://localhost:8080` (`airflow` /
      `airflow`).
    - **Onde ficam os segredos?** Em Connections lidas de variáveis de ambiente
      do `.env`, nunca no repositório.

## Serviços do stack

| Serviço | Função | Porta exposta |
|---|---|---|
| `mongodb` | Origem local dos dados (MongoDB 7) | `27017` |
| `minio` | Object storage compatível com S3 (data lake) | `9000` (API), `9001` (console) |
| `airflow-postgres` | Banco de metadados do Airflow (Postgres 16) | — |
| `airflow-init` | Job único: migra o banco e cria o usuário inicial | — |
| `airflow-apiserver` | Interface web e API do Airflow | `8080` |
| `airflow-scheduler` | Agenda e dispara as tasks | — (health interno `8974`) |
| `airflow-dag-processor` | Processa e importa os arquivos de DAG | — |
| `airflow-triggerer` | Executa tarefas *deferrable* (assíncronas) | — |

!!! info "Por que tantos containers de Airflow?"
    A partir do Airflow 3, o parsing das DAGs (`dag-processor`), o agendamento
    (`scheduler`), a API/UI (`apiserver`) e os *triggers* assíncronos
    (`triggerer`) rodam em processos **separados**. Isolá-los em containers
    distintos reflete a arquitetura oficial e facilita ler os logs de cada
    responsabilidade.

### Tabela de portas

| Porta | Serviço | Para quê |
|---|---|---|
| `8080` | `airflow-apiserver` | Interface web e API REST do Airflow |
| `9000` | `minio` | Endpoint S3 (usado pelas DAGs e pelo Spark) |
| `9001` | `minio` | Console web do MinIO (gerenciar buckets) |
| `27017` | `mongodb` | Conexão com o MongoDB de origem local |

## Dependências entre os serviços

```mermaid
flowchart TD
    pg["airflow-postgres\n(metadados)"] -->|service_healthy| init["airflow-init\n(migra DB + cria usuário)"]
    init -->|service_completed_successfully| api["airflow-apiserver\n:8080"]
    init --> sched["airflow-scheduler"]
    init --> dagp["airflow-dag-processor"]
    init --> trig["airflow-triggerer"]
    mongo[("mongodb :27017")] -. origem .-> sched
    minio[("minio :9000/:9001")] -. destino .-> sched
```

A ordem é garantida por `depends_on` com **condições de saúde**: o `airflow-init`
só começa quando o Postgres está `healthy`, e os componentes do Airflow só
sobem depois que o `init` termina com sucesso. Os trechos abaixo mostram esse
encadeamento no `docker-compose.yml`:

```yaml title="docker-compose.yml — depends_on comum"
--8<-- "docker-compose.yml:37:39"
```

```yaml title="docker-compose.yml — airflow-init"
--8<-- "docker-compose.yml:104:119"
```

```yaml title="docker-compose.yml — apiserver depende do init"
--8<-- "docker-compose.yml:134:137"
```

## Configuração comum (`x-airflow-common`)

Todos os componentes do Airflow compartilham a mesma imagem, variáveis de
ambiente e volumes via âncora YAML `&airflow-common`. É aqui que o executor, a
conexão com o Postgres de metadados e as variáveis das DAGs ficam definidas:

```yaml title="docker-compose.yml — bloco comum"
--8<-- "docker-compose.yml:9:36"
```

A imagem é construída a partir de um `Dockerfile.airflow` enxuto, que apenas
instala os providers necessários sobre a imagem oficial, respeitando o arquivo
de *constraints* da versão:

```dockerfile title="Dockerfile.airflow"
--8<-- "Dockerfile.airflow"
```

## Arquivos da entrega

| Arquivo | Responsabilidade |
|---|---|
| `docker-compose.yml` | Stack local com MongoDB, MinIO, Postgres e Airflow |
| `Dockerfile.airflow` | Imagem customizada baseada em `apache/airflow` |
| `requirements-airflow.txt` | Providers de MongoDB e Amazon/S3 usados pelas DAGs |
| `.env.example` | Variáveis locais, usuário inicial e Connections do Airflow |
| `.gitignore` | Ignora logs e configuração local gerada pelo Airflow |

## Subir o ambiente

Primeiro, crie o arquivo local de variáveis (o `.env` não é versionado):

```bash
cp .env.example .env
```

Suba o stack completo. Os componentes do Airflow puxam suas dependências
(`airflow-init` e `airflow-postgres`) automaticamente:

```bash
docker compose up -d --build \
  mongodb \
  minio \
  airflow-apiserver \
  airflow-scheduler \
  airflow-dag-processor \
  airflow-triggerer
```

!!! note "O que o `airflow-init` faz"
    Ele roda **uma vez** como dependência: aplica as migrações do banco de
    metadados (`_AIRFLOW_DB_MIGRATE`) e cria o usuário inicial da interface
    (`_AIRFLOW_WWW_USER_CREATE`). Ao terminar, libera os demais containers.

Acompanhe o status até todos ficarem `healthy`:

```bash
docker compose ps
```

## Acessar a interface

Abra a interface web do Airflow:

```text
http://localhost:8080
```

Credenciais padrão definidas no `.env.example`:

| Campo | Variável | Valor padrão |
|---|---|---|
| Usuário | `_AIRFLOW_WWW_USER_USERNAME` | `airflow` |
| Senha | `_AIRFLOW_WWW_USER_PASSWORD` | `airflow` |

!!! warning "Troque a senha se expor o ambiente"
    As credenciais padrão são **apenas para uso local**. Se o ambiente ficar
    acessível fora da sua máquina, altere `_AIRFLOW_WWW_USER_PASSWORD` (e o
    `AIRFLOW__API_AUTH__JWT_SECRET`) no seu `.env`.

## Connections

As Connections são criadas a partir de **variáveis de ambiente**
(`AIRFLOW_CONN_*`), então nenhuma credencial é salva no repositório. Por padrão,
elas apontam para os serviços Docker `mongodb` e `minio`:

| Connection Id | Variável | Padrão local |
|---|---|---|
| `mongodb_atlas` | `AIRFLOW_CONN_MONGODB_ATLAS` | MongoDB Docker em `mongodb:27017` |
| `minio_s3` | `AIRFLOW_CONN_MINIO_S3` | MinIO Docker em `minio:9000` |

```dotenv title=".env.example — Connections"
--8<-- ".env.example:68:72"
```

Para fazer a DAG ler do **Atlas** em vez do MongoDB local, sobrescreva a
Connection no seu `.env` (ver [MongoDB Atlas](mongodb_atlas.md)):

```bash
AIRFLOW_CONN_MONGODB_ATLAS=mongo://USUARIO:SENHA@cluster.mongodb.net/ecommerce?srv=true&ssl=true&authSource=admin&retryWrites=true&w=majority
```

## Variáveis por DAG

O bloco comum também injeta as variáveis que **parametrizam cada DAG** do
pipeline. Assim, agendamentos, buckets e listas de tabelas ficam configuráveis
sem editar código:

| DAG (Issue) | Variáveis principais | Para quê |
|---|---|---|
| `mongodb_to_landing` (#15) | `MONGO_DATABASE`, `MONGO_COLLECTIONS`, `MONGO_INCREMENTAL_FIELD`, `MONGO_BATCH_SIZE`, `MONGO_CHECKPOINT_OVERLAP_HOURS`, `MONGO_TO_LANDING_SCHEDULE`, `LANDING_BUCKET` | Extração incremental do MongoDB para a Landing no MinIO |
| `landing_to_bronze` (#16) | `LANDING_TO_BRONZE_APPLICATION`, `LANDING_TO_BRONZE_SCHEDULE`, `SPARK_CONN_ID`, `SPARK_S3_ENDPOINT`, `SPARK_PACKAGES` | Conversão da Landing em tabelas Delta na Bronze (Spark) |
| `bronze_to_silver` (#17) | `SILVER_TABLES`, `BRONZE_TO_SILVER_APPLICATION`, `BRONZE_TO_SILVER_SCHEDULE` | Limpeza/normalização da Bronze para a Silver |
| `silver_to_gold` (#18) | `GOLD_TABLES`, `SILVER_TO_GOLD_APPLICATION`, `SILVER_TO_GOLD_SCHEDULE` | Modelagem dimensional (dimensões e fatos) na Gold |

As variáveis da DAG `mongodb_to_landing` vêm definidas assim no `.env.example`:

```dotenv title=".env.example — DAG MongoDB → Landing"
--8<-- ".env.example:17:29"
```

!!! tip "Agendamentos escalonados"
    Repare nos *crons* das DAGs Spark (`5-59/15`, `10-59/15`, `15-59/15`): cada
    etapa começa alguns minutos **depois** da anterior, dando tempo para a
    camada predecessora terminar de gravar antes da próxima iniciar.

## Validar a importação das DAGs

Depois que os containers estiverem saudáveis, confirme que as DAGs foram
importadas sem erros de sintaxe ou de dependência:

```bash
# lista erros de importacao (deve sair vazio)
docker compose exec airflow-apiserver airflow dags list-import-errors

# lista as DAGs reconhecidas
docker compose exec airflow-apiserver airflow dags list
```

Para testar a DAG `mongodb_to_landing` de ponta a ponta, garanta antes que:

1. o MongoDB esteja populado (ver [MongoDB Atlas](mongodb_atlas.md));
2. o bucket e os prefixos da Landing existam no MinIO;
3. as Connections apontem para a origem desejada.

Com tudo preparado, rode um teste de execução para uma data lógica:

```bash
docker compose exec airflow-apiserver airflow dags test mongodb_to_landing 2026-06-13
```

## Parar ou limpar

=== "Parar mantendo os dados"

    ```bash
    docker compose down
    ```

=== "Parar e apagar os dados locais"

    ```bash
    # remove volumes: MongoDB, MinIO e metadados do Airflow
    docker compose down -v
    ```

!!! warning "`down -v` apaga tudo"
    A flag `-v` remove os volumes `mongo_data`, `minio_data` e
    `airflow_postgres_data`. Você perderá os dados carregados e o histórico de
    execuções do Airflow — use apenas para começar do zero.

## Referências

- [Apache Airflow — Documentação](https://airflow.apache.org/docs/)
- [Running Airflow in Docker (guia oficial)](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [Docker Compose](https://docs.docker.com/compose/)
- [MinIO — Documentação](https://min.io/docs/minio/container/index.html)
- Página completa de [referências](referencias.md)
