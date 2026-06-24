---
tags:
  - mongodb
  - atlas
  - origem
---

# MongoDB Atlas — origem compartilhada

Para que todos os integrantes da equipe acessem a **mesma** base de origem, o
MongoDB é disponibilizado em um cluster gratuito (**M0**) no
[MongoDB Atlas](https://www.mongodb.com/atlas). Assim, o pipeline da camada
Medalhão sempre parte de um conjunto de dados idêntico, independentemente de
quem executa as DAGs.

O ponto central é que **o mesmo script de carga**
([`carregar_mongo.py`](modelo_mongodb.md)) é usado tanto no Docker local quanto
no Atlas — **muda apenas o `MONGO_URI`**. Isso evita manter duas rotinas
diferentes e garante que os validadores e índices sejam idênticos nos dois
ambientes.

!!! abstract "Em resumo"
    - **Por que Atlas?** Uma origem única e compartilhada elimina o problema de
      "na minha máquina funciona": todos leem os mesmos 150.000 documentos.
    - **Por que M0?** É o tier gratuito do Atlas, suficiente para um projeto
      acadêmico, sem custo e sem cartão de crédito.
    - **Como alternar local ↔ Atlas?** Trocando uma única variável, o
      `MONGO_URI`, no seu `.env`. Nenhuma mudança de código.

!!! warning "Segredos nunca vão para o Git"
    A connection string contém **usuário e senha**. Ela vai **somente** no
    `.env` local (que está no `.gitignore`) — **nunca** em commits, prints,
    mensagens no chat ou no código. Se uma senha vazar, troque-a imediatamente
    em **Database Access** no Atlas.

## Visão geral do fluxo

```mermaid
flowchart LR
    dataset["dataset/arquivos_csv\n(10 coleções)"] --> loader["carregar_mongo.py\n(idempotente)"]
    env[".env\nMONGO_URI"] -. define destino .-> loader
    loader -->|MONGO_URI local| local[("MongoDB local\nDocker :27017")]
    loader -->|MONGO_URI Atlas| atlas[("MongoDB Atlas\nM0 cluster")]
    atlas --> airflow["DAG mongodb_to_landing\n(Airflow)"]
    local --> airflow
```

O destino (local ou Atlas) é decidido **em tempo de execução** pela URI lida do
`.env`; o restante da carga — validação de schema e criação de índices — é
exatamente o mesmo.

## Passo a passo

=== "1. Criar o cluster"

    1. Crie a conta / faça login em
       <https://www.mongodb.com/cloud/atlas/register>.
    2. **Create a cluster** → selecione o tier **M0 (Free)**.
    3. Provider **AWS**, region **São Paulo (sa-east-1)** — manter a região
       próxima reduz a latência da carga.
    4. Nomeie (ex.: `ecommerce-origem`) e clique em **Create Deployment**.

=== "2. Usuário do banco"

    Em **Security → Database Access → Add New Database User**:

    - Autenticação **Password** (SCRAM).
    - Defina um usuário (ex.: `app_user`) e uma senha forte.

    !!! warning "Caracteres que quebram a URI"
        Evite os caracteres `@ : / ? #` na senha — eles têm significado especial
        na connection string. Se precisar usá-los, faça **URL-encode** (ex.:
        `@` vira `%40`) ao montar o `MONGO_URI`.

=== "3. Liberar o acesso de rede"

    Em **Security → Network Access → + ADD IP ADDRESS**:

    - Para o time inteiro acessar de qualquer lugar: **ALLOW ACCESS FROM
      ANYWHERE** (`0.0.0.0/0`) → **Confirm** e aguarde o status **Active**.

    !!! info "Trade-off de segurança"
        Com `0.0.0.0/0` o cluster fica alcançável de qualquer IP, mas o acesso
        continua exigindo usuário e senha. Alternativa mais restrita: cada
        integrante adiciona apenas o **próprio IP**. Para um projeto acadêmico
        de curta duração, liberar geral simplifica o trabalho em equipe.

=== "4. Obter a connection string"

    No cluster → **Connect** → **Drivers** → copie a URI no formato `+srv`:

    ```text
    mongodb+srv://app_user:<db_password>@ecommerce-origem.xxxxx.mongodb.net/?retryWrites=true&w=majority
    ```

    Substitua `<db_password>` pela senha real. O esquema `mongodb+srv://` resolve
    automaticamente os nós do cluster via DNS (por isso depende do `dnspython`,
    ver passo 6).

## Configurar o `.env`

Cada integrante coloca a URI no **seu** `.env`, a partir do `.env.example`:

```bash
cp .env.example .env
```

O `.env.example` já documenta os dois cenários (local e Atlas). A linha do
Atlas vem **comentada de propósito** — você a preenche no seu `.env` privado:

```dotenv title=".env.example"
--8<-- ".env.example:9:15"
```

No seu `.env`, deixe ativa apenas a URI do destino desejado:

```dotenv title=".env (exemplo Atlas)"
MONGO_DB=ecommerce
MONGO_URI=mongodb+srv://app_user:SUASENHA@ecommerce-origem.xxxxx.mongodb.net/?retryWrites=true&w=majority
```

## Carregar os dados no Atlas

Conexões `mongodb+srv://` exigem o `dnspython` (já listado no
`pyproject.toml`, grupo `[dataset]`), usado para resolver os registros SRV do
cluster:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install ".[dataset]"

# le o MONGO_URI do .env e carrega as 10 colecoes no Atlas
python dataset/scripts_py/carregar_mongo.py
```

!!! tip "Idempotência"
    O script **recria** as coleções a cada execução e reaplica os mesmos
    validadores e índices da carga local. Rodar duas vezes não duplica dados —
    o resultado final é sempre o mesmo. Saída esperada:

    ```text
    Concluido: 150,000 documentos em 10 colecao(oes).
    ```

## Alternar entre Atlas e local

Como o destino é definido pelo `MONGO_URI`, alternar entre os ambientes é
apenas uma questão de qual URI está ativa. Você pode editar o `.env` ou passar
`--uri` diretamente na linha de comando:

=== "Atlas (padrão do .env)"

    ```bash
    # usa o MONGO_URI definido no .env
    python dataset/scripts_py/carregar_mongo.py
    ```

=== "Local (Docker)"

    ```bash
    python dataset/scripts_py/carregar_mongo.py \
      --uri "mongodb://admin:admin123@localhost:27017/?authSource=admin"
    ```

!!! note "Por que o `authSource=admin` no local?"
    No Docker, o usuário root é criado no banco `admin`
    (`MONGO_INITDB_ROOT_USERNAME`), então a autenticação precisa apontar para
    lá. No Atlas o usuário é criado diretamente no projeto, dispensando esse
    parâmetro.

## Atlas e o Airflow

O Airflow não usa o `MONGO_URI` dos scripts: ele lê uma **Connection** própria,
`mongodb_atlas`, definida pela variável `AIRFLOW_CONN_MONGODB_ATLAS`. Para
apontar a DAG `mongodb_to_landing` ao Atlas em vez do MongoDB local, sobrescreva
essa variável no seu `.env`. Detalhes na página
[Ambiente Airflow local](ambiente_airflow.md).

## Referências

- [MongoDB Atlas — Documentação](https://www.mongodb.com/docs/atlas/)
- [Connection strings (formato `+srv`)](https://www.mongodb.com/docs/manual/reference/connection-string/)
- [PyMongo — conexões SRV / alta disponibilidade](https://pymongo.readthedocs.io/en/stable/examples/high_availability.html)
- Página completa de [referências](referencias.md)
