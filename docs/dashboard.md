---
tags:
  - dashboard
  - power bi
  - gold
  - kpis
---

# Dashboard (Power BI)

O dashboard consome o **modelo dimensional da camada Gold**. Como o Power BI não
lê Delta Lake no MinIO nativamente — e o Power BI Desktop só roda no Windows —
exportamos a Gold para **CSV** e importamos no Power BI (Desktop ou
**Power BI Service** na web, ideal para quem está no macOS/Linux).

!!! abstract "Em resumo"
    - **Ponte:** `scripts/exportar_gold.py` reconstrói as 4 dimensões + 4 fatos
      (mesmos builders da DAG Silver → Gold) e grava **um CSV por tabela** em
      `gold_export/`.
    - **Modelo:** esquema estrela — os fatos ligam às dimensões por
      `cliente_key`, `produto_key`, `cupom_key` e `data_key`.
    - **Entrega (PDF):** One Page View com **4 KPIs + 2 métricas**.

## 1. Exportar a Gold para CSV

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install ".[spark]"
python scripts/exportar_gold.py
```

Gera em `gold_export/`: `dim_tempo`, `dim_cliente`, `dim_produto`, `dim_cupom`,
`fato_vendas`, `fato_pagamentos`, `fato_entregas`, `fato_avaliacoes` (`.csv`).

??? info "Como o script funciona"
    Ele lê os CSVs da origem (`dataset/arquivos_csv/`), tipa as colunas e chama
    os mesmos `MODEL_BUILDERS` de `spark_jobs/silver_to_gold.py` — ou seja, é o
    modelo dimensional **real**, só que materializado localmente (sem
    MinIO/Airflow) para facilitar a visualização.

    ```python title="scripts/exportar_gold.py"
    --8<-- "scripts/exportar_gold.py:55:78"
    ```

## 2. Importar no Power BI

=== "Power BI Service (web — macOS/Linux)"

    1. Acesse <https://app.powerbi.com> → workspace → **New** → **Semantic model**
       / **Upload** → **Get data** → **Files** → **Local file**.
    2. Envie cada CSV de `gold_export/`.
    3. Monte o relatório (One Page View) com os visuais.

=== "Power BI Desktop (Windows)"

    1. **Get data → Text/CSV** e selecione cada arquivo de `gold_export/`.
    2. Construa as relações na aba **Model**.

## 3. Relações (esquema estrela)

| Fato (coluna) | → | Dimensão (chave) |
|---|---|---|
| `fato_*.cliente_key` | → | `dim_cliente.cliente_key` |
| `fato_vendas.produto_key`, `fato_avaliacoes.produto_key` | → | `dim_produto.produto_key` |
| `fato_vendas.cupom_key` | → | `dim_cupom.cupom_key` |
| `fato_*.data_key` | → | `dim_tempo.data_key` |

!!! tip "Cardinalidade"
    Todas são **N:1** (muitos fatos para uma dimensão), com a dimensão no lado
    "1". Deixe `dim_tempo` como a tabela de calendário para análises por período.

## 4. KPIs e métricas (One Page View)

Medidas sugeridas (DAX), conforme definido para o projeto:

### KPIs

| KPI | Medida (DAX) |
|---|---|
| **Faturamento total** | `Faturamento = SUM(fato_vendas[receita_liquida])` |
| **Qtd. de pedidos** | `Pedidos = DISTINCTCOUNT(fato_vendas[id_pedido])` |
| **Ticket médio** | `Ticket Medio = DIVIDE([Faturamento], [Pedidos])` |
| **Taxa de entregas no prazo** | `Entregas no Prazo % = DIVIDE(CALCULATE(COUNTROWS(fato_entregas), fato_entregas[entrega_no_prazo] = TRUE()), COUNTROWS(fato_entregas))` |

### Métricas

| Métrica | Como montar |
|---|---|
| **Faturamento por mês** | gráfico de linha: `[Faturamento]` por `dim_tempo[ano_mes]` |
| **Produtos mais vendidos** | gráfico de barras: `SUM(fato_vendas[quantidade])` por `dim_produto[nome_produto]` (Top N) |

Filtros recomendados (One Page View): período (`dim_tempo`), categoria/marca
(`dim_produto`), estado (`dim_cliente`) e forma de pagamento (`fato_pagamentos`).

## Referências

- [Power BI — Get data de arquivos](https://learn.microsoft.com/power-bi/connect-data/service-comma-separated-value-files)
- [DAX — referência de funções](https://learn.microsoft.com/dax/)
- Modelo dimensional: [DAG Silver → Gold](dag_silver_gold.md)
- Página completa de [referências](referencias.md)
