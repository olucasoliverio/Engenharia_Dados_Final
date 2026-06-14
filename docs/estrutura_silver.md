# Estrutura da camada Silver

Implementação da **Issue #12**, responsável por preparar a camada Silver no
MinIO ou Amazon S3 para receber os dados limpos e padronizados do e-commerce.

## Objetivo

A Silver mantém a granularidade das entidades da Bronze, mas representa a fonte
corporativa validada para os próximos consumidores. Esta entrega cria:

- contrato versionado das dez tabelas Silver;
- prefixos das tabelas no bucket `datalake`;
- marcadores ocultos `_READY`;
- manifesto de controle da camada;
- validação automatizada e inicialização idempotente;
- infraestrutura comum reutilizável pelas camadas Delta.

## Contrato versionado

O arquivo `config/silver_structure.json` é a fonte de verdade:

```json
{
  "bucket": "datalake",
  "database": "ecommerce",
  "layer": "silver",
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
  "partition_columns": []
}
```

As dez tabelas permanecem alinhadas às entidades da Bronze.

Nenhuma coluna de particionamento global é definida nesta estrutura. As tabelas
possuem datas de negócio diferentes e a estratégia física será definida pela
DAG Bronze → Silver conforme as regras de cada entidade.

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

python scripts/criar_estrutura_silver.py
```

Resultado esperado:

```text
Estrutura criada/atualizada: 10 tabelas em s3://datalake/silver/
Marcadores criados: 10; manifesto atualizado: sim
Estrutura Silver valida: s3://datalake/silver/
```

Na segunda execução, o comando deve informar:

```text
Marcadores criados: 0; manifesto atualizado: nao
```

## Estrutura criada

```text
s3://datalake/
└── silver/
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

O manifesto registra o formato `delta`, a origem `bronze`, os prefixos das
tabelas e os caminhos esperados para os respectivos `_delta_log`.

## Delta Lake

Os marcadores `_READY` não simulam tabelas Delta. A DAG Bronze → Silver criará
os logs transacionais e arquivos de dados na primeira gravação:

```text
silver/ecommerce/<tabela>/_delta_log/
silver/ecommerce/<tabela>/*.parquet
```

A DAG `bronze_to_silver` aplica deduplicação, tratamento de nulos,
padronização de tipos, formatação de datas e validações de qualidade. A
implementação está documentada em
[`dag_bronze_silver.md`](dag_bronze_silver.md).

## Validação

Para verificar a estrutura sem modificar objetos:

```bash
python scripts/criar_estrutura_silver.py --validate-only
```

O comando retorna código `0` quando os dez marcadores e o manifesto existem e
correspondem ao contrato.

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
| Objetos sob `silver/` | 11 |
| Versões sob `silver/` | 11 |
| Versionamento do bucket | `Enabled` |
| Segunda execução | 0 marcadores e 0 manifestos alterados |

A igualdade entre objetos e versões confirma que a segunda execução não gerou
novas versões. A Bronze também foi validada novamente após a extração do módulo
compartilhado para estruturas Delta.

## Limites desta issue

Esta entrega prepara e valida o armazenamento da Silver. A leitura da Bronze,
as regras de qualidade e a gravação física das tabelas Delta são implementadas
separadamente pela Issue #17.
