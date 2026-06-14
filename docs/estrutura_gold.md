# Estrutura da camada Gold

Implementação da **Issue #13**, responsável por preparar a camada Gold no
MinIO ou Amazon S3 para o modelo analítico consumido pelas ferramentas de BI.

## Objetivo

A Gold organiza os dados validados da Silver em um modelo dimensional voltado
a vendas, pagamentos, logística e satisfação. Esta entrega cria:

- contrato versionado do modelo analítico;
- quatro dimensões e quatro tabelas fato;
- marcadores ocultos `_READY`;
- manifesto de controle da camada;
- validação automatizada e inicialização idempotente.

## Modelo dimensional

O arquivo `config/gold_structure.json` define oito tabelas:

| Tabela | Tipo | Finalidade |
|---|---|---|
| `dim_tempo` | Dimensão | Calendário para análises temporais |
| `dim_cliente` | Dimensão | Perfil e localização dos clientes |
| `dim_produto` | Dimensão | Produto, marca, categoria e fornecedor |
| `dim_cupom` | Dimensão | Campanhas e descontos |
| `fato_vendas` | Fato | Pedidos, itens, quantidade, desconto e receita |
| `fato_pagamentos` | Fato | Valores, formas, status e parcelas |
| `fato_entregas` | Fato | Prazo, atraso, transportadora e status |
| `fato_avaliacoes` | Fato | Nota e volume de avaliações |

Categoria e fornecedor são incorporados à dimensão de produto pela
[DAG Silver → Gold](dag_silver_gold.md). Isso simplifica o consumo no BI e
evita exigir múltiplos joins para as análises mais comuns.

Nenhum particionamento global é definido nesta etapa. A DAG Silver → Gold
mantém as dimensões sem particionamento e particiona as quatro tabelas fato
pela coluna `ano`.

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

python scripts/criar_estrutura_gold.py
```

Resultado esperado:

```text
Estrutura criada/atualizada: 8 tabelas em s3://datalake/gold/
Marcadores criados: 8; manifesto atualizado: sim
Estrutura Gold valida: s3://datalake/gold/
```

Na segunda execução:

```text
Marcadores criados: 0; manifesto atualizado: nao
```

## Estrutura criada

```text
s3://datalake/
└── gold/
    ├── ecommerce/
    │   ├── dim_tempo/_READY
    │   ├── dim_cliente/_READY
    │   ├── dim_produto/_READY
    │   ├── dim_cupom/_READY
    │   ├── fato_vendas/_READY
    │   ├── fato_pagamentos/_READY
    │   ├── fato_entregas/_READY
    │   └── fato_avaliacoes/_READY
    └── _control/
        └── _structure.json
```

Os marcadores reservam os prefixos, mas não criam tabelas Delta vazias.
A primeira gravação da DAG Silver → Gold cria os arquivos Parquet e o
`_delta_log` de cada modelo.

## Indicadores suportados

O modelo foi organizado para permitir, entre outros:

- receita, pedidos, itens vendidos e ticket médio;
- vendas por período, estado, produto, marca e categoria;
- desempenho de cupons e descontos;
- pagamentos por forma e status;
- prazo médio, atraso e desempenho por transportadora;
- nota média e avaliações por produto.

As medidas necessárias para esses indicadores são materializadas pela
DAG Silver → Gold e agregadas no consumo pelo Power BI ou Superset.

## Validação

Para verificar a estrutura sem modificar objetos:

```bash
python scripts/criar_estrutura_gold.py --validate-only
```

Testes automatizados:

```bash
PYTHONPYCACHEPREFIX=/tmp/engenharia_dados_pycache \
  python3 -m unittest discover -s tests -v
```

### Validação integrada local

Em 13 de junho de 2026, a estrutura foi criada e validada no MinIO local:

| Verificação | Resultado |
|---|---|
| Dimensões previstas | 4 |
| Tabelas fato previstas | 4 |
| Marcadores `_READY` | 8 |
| Manifestos de controle | 1 |
| Objetos sob `gold/` | 9 |
| Versões sob `gold/` | 9 |
| Versionamento do bucket | `Enabled` |
| Segunda execução | 0 marcadores e 0 manifestos alterados |

A igualdade entre objetos e versões confirma que a segunda execução não gerou
novas versões.

## Limites desta issue

Esta entrega prepara e valida o armazenamento da Gold. A implementação dos
joins, dimensões, fatos e medidas de negócio está documentada separadamente na
[Issue #18](dag_silver_gold.md).
