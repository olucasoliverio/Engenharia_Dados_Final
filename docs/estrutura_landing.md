# Estrutura da camada Landing

Implementação da **Issue #10**, atualizada para o domínio de e-commerce usado
pelo projeto. Os critérios antigos da issue, relacionados a alunos e check-ins,
não correspondem ao modelo MongoDB atual.

## Objetivo

Disponibilizar uma camada Landing reproduzível em MinIO ou Amazon S3 para
receber os documentos brutos extraídos pela DAG `mongodb_to_landing`.

A entrega inclui:

- MinIO local via Docker Compose;
- bucket `datalake`;
- versionamento habilitado;
- prefixos das dez coleções do e-commerce;
- manifesto de controle da estrutura;
- inicialização idempotente;
- comando de validação sem alterações.

## MinIO local

O serviço usa uma versão fixa da imagem para garantir reprodutibilidade:

```text
minio/minio:RELEASE.2025-09-07T16-13-09Z
```

Suba somente o object storage:

```bash
cp .env.example .env
docker compose up -d minio
docker compose ps minio
```

Serviços disponíveis:

| Serviço | Endereço |
|---|---|
| API compatível com S3 | <http://localhost:9000> |
| Console administrativo | <http://localhost:9001> |
| Healthcheck | <http://localhost:9000/minio/health/live> |

As credenciais locais são definidas por `MINIO_ROOT_USER` e
`MINIO_ROOT_PASSWORD`. Os valores do `.env.example` são apenas para
desenvolvimento local.

## Contrato versionado

O arquivo `config/landing_structure.json` é a fonte de verdade da estrutura:

```json
{
  "bucket": "datalake",
  "database": "ecommerce",
  "layer": "landing",
  "collections": [
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
  ]
}
```

Os testes garantem que essa lista permaneça igual à lista padrão da DAG da
Issue #15.

## Inicialização

Instale a dependência do inicializador:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-infra.txt
```

Exporte as variáveis do `.env` e execute:

```bash
set -a
source .env
set +a

python scripts/criar_estrutura_landing.py
```

Resultado esperado:

```text
Estrutura criada/atualizada: 10 colecoes em s3://datalake/landing/
Estrutura Landing valida: s3://datalake/landing/
```

O comando é idempotente e pode ser executado novamente sem duplicar objetos ou
criar novas versões quando o contrato não mudou.

## Estrutura criada

Object storages não possuem diretórios reais. Os prefixos são materializados
por objetos vazios `_READY`:

```text
s3://datalake/
└── landing/
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

Os arquivos de dados da DAG são adicionados abaixo desses mesmos prefixos:

```text
landing/ecommerce/<colecao>/extraction_date=AAAA-MM-DD/
  run_id=<airflow_run_id>/part-00000.json
```

O objeto `landing/_control/_structure.json` registra coleções, prefixos,
formato esperado e data de geração.

## Validação

Para verificar a estrutura sem criar ou modificar objetos:

```bash
python scripts/criar_estrutura_landing.py --validate-only
```

O comando retorna código `0` quando todos os objetos obrigatórios existem e
código `1` quando o bucket ou algum marcador está ausente.

Testes automatizados:

```bash
PYTHONPYCACHEPREFIX=/tmp/engenharia_dados_pycache \
  python3 -m unittest discover -s tests -v
```

### Validação integrada local

Em 13 de junho de 2026, a estrutura foi validada localmente com MongoDB 7,
MinIO e Apache Airflow 3.2.2:

| Execução | Arquivos JSON | Documentos | Resultado |
|---|---:|---:|---|
| Carga inicial | 10 | 150.000 | Sucesso |
| Carga incremental | 10 | 635 | Sucesso |

A carga inicial gravou 15.000 documentos de cada coleção e gerou um manifesto
com total de 150.000. A segunda execução leu somente a janela incremental de 24
horas, gerando 635 documentos, exatamente o volume previsto pela consulta na
origem.

Também foram confirmados:

- dez checkpoints no Airflow, um por coleção;
- dois manifestos de execução no MinIO;
- 150.000 linhas na carga inicial e 635 na incremental;
- MongoDB e MinIO com healthcheck saudável;
- checkpoints inalterados na segunda execução, pois a origem não recebeu novos
  registros entre as cargas.

## Amazon S3

O mesmo inicializador funciona no Amazon S3 usando a cadeia padrão de
credenciais do `boto3`. Para isso, não defina `S3_ENDPOINT_URL` e configure as
credenciais e região da conta AWS.

## Limites desta issue

Esta entrega cria e valida a estrutura de armazenamento. A extração MongoDB →
Landing pertence à Issue #15, e a conversão dos arquivos para Delta Lake na
Bronze pertence à Issue #16.

## Referências

- [MinIO — Documentação](https://min.io/docs/minio/linux/index.html)
- [MongoDB Extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/)
- Página completa de [referências](referencias.md)
