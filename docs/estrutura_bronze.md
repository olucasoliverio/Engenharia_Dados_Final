# Estrutura da camada Bronze

Implementação da **Issue #11**, responsável por preparar a camada Bronze no
MinIO ou Amazon S3 para receber as tabelas Delta Lake do e-commerce.

## Objetivo

A Bronze preserva a granularidade dos documentos da Landing, adicionando
histórico e metadados técnicos. Esta entrega cria:

- contrato versionado das dez tabelas;
- prefixos das tabelas no bucket `datalake`;
- marcadores ocultos `_READY`;
- manifesto de controle da camada;
- particionamento previsto por `ingestion_date`;
- validação automatizada e inicialização idempotente.

## Contrato versionado

O arquivo `config/bronze_structure.json` é a fonte de verdade:

```json
{
  "bucket": "datalake",
  "database": "ecommerce",
  "layer": "bronze",
  "tables": [
    "clientes",
    "categorias",
    "fornecedores",
    "produtos",
    "cupons",
    "pedidos",
    "itens_pedido",
    "pagamentos",
    "entregas",
    "avaliacoes"
  ],
  "partition_columns": [
    "ingestion_date"
  ]
}
```

Os testes garantem que as tabelas permaneçam alinhadas às dez coleções
processadas pela DAG MongoDB → Landing.

## Inicialização

Com o MinIO ativo e as variáveis do `.env` exportadas:

```bash
docker compose up -d minio

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-infra.txt

set -a
source .env
set +a

python scripts/criar_estrutura_bronze.py
```

Resultado esperado:

```text
Estrutura criada/atualizada: 10 tabelas em s3://datalake/bronze/
Marcadores criados: 10; manifesto atualizado: sim
Estrutura Bronze valida: s3://datalake/bronze/
```

O comando pode ser executado novamente sem duplicar objetos ou criar novas
versões quando o contrato não mudou. Na segunda execução, a linha de auditoria
informa `Marcadores criados: 0; manifesto atualizado: nao`.

## Estrutura criada

```text
s3://datalake/
└── bronze/
    ├── ecommerce/
    │   ├── clientes/_READY
    │   ├── categorias/_READY
    │   ├── fornecedores/_READY
    │   ├── produtos/_READY
    │   ├── cupons/_READY
    │   ├── pedidos/_READY
    │   ├── itens_pedido/_READY
    │   ├── pagamentos/_READY
    │   ├── entregas/_READY
    │   └── avaliacoes/_READY
    └── _control/
        └── _structure.json
```

O manifesto registra, para cada tabela:

- prefixo raiz da tabela;
- formato `delta`;
- origem na camada Landing;
- colunas de particionamento;
- caminho esperado do `_delta_log`.

## Delta Lake

O marcador `_READY` apenas materializa o prefixo no object storage. Ele não
simula uma tabela Delta e não cria um log de transações vazio.

A tabela passa a existir como Delta Lake quando a DAG Landing → Bronze realiza
a primeira gravação e cria:

```text
bronze/ecommerce/<tabela>/_delta_log/
bronze/ecommerce/<tabela>/ingestion_date=AAAA-MM-DD/*.parquet
```

Essa separação evita criar tabelas sem esquema e mantém a conversão dos JSONs
para Delta dentro da Issue #16.

## Validação

Para verificar a camada sem criar ou modificar objetos:

```bash
python scripts/criar_estrutura_bronze.py --validate-only
```

O comando retorna código `0` quando os dez marcadores e o manifesto existem e
estão de acordo com o contrato. Um manifesto desatualizado também é informado
como divergência.

Testes automatizados:

```bash
PYTHONPYCACHEPREFIX=/tmp/engenharia_dados_pycache \
  python3 -m unittest discover -s tests -v
```

### Validação integrada local

Em 13 de junho de 2026, a estrutura foi criada e validada no MinIO local:

| Verificação | Resultado |
|---|---|
| Tabelas previstas | 10 |
| Marcadores `_READY` | 10 |
| Manifestos de controle | 1 |
| Objetos sob `bronze/` | 11 |
| Versões sob `bronze/` | 11 |
| Versionamento do bucket | `Enabled` |
| Segunda execução | 0 marcadores e 0 manifestos alterados |

A igualdade entre objetos e versões confirma que a segunda execução não gerou
novas versões. O modo `--validate-only` também confirmou que a estrutura e o
manifesto permanecem de acordo com o contrato versionado.

## Limites desta issue

Esta entrega prepara e valida o armazenamento da Bronze. A leitura da Landing,
a conversão com Apache Spark e a criação física das tabelas Delta pertencem à
Issue #16.
