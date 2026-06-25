---
tags:
  - dashboard
  - looker studio
  - gold
  - kpis
---

# Dashboard (Looker Studio)

O dashboard consome o **modelo dimensional da camada Gold**. Para visualizar,
exportamos a Gold para **CSV** e importamos no **Looker Studio** (gratuito,
100% web — funciona em qualquer SO, inclusive macOS).

!!! abstract "Em resumo"
    - **Ponte:** `scripts/exportar_gold.py` reconstrói a Gold e grava CSVs em
      `gold_export/`, incluindo a **`obt_vendas.csv`** (tabela única achatada).
    - **Caminho simples:** no Looker Studio importa-se **só a `obt_vendas.csv`** —
      uma tabela, **sem criar relações**.
    - **Entrega (PDF):** One Page View com **4 KPIs + 2 métricas** + filtros.

## 1. Exportar a Gold para CSV

```bash
uv venv && source .venv/bin/activate
uv pip install ".[spark]"
python scripts/exportar_gold.py
```

Gera em `gold_export/` o **esquema estrela** (4 dimensões + 4 fatos) **e** a
**`obt_vendas.csv`** (1 linha por item de pedido, com cliente, produto,
categoria, valores, pagamento e entrega já juntos).

??? info "Como o script funciona"
    Lê os CSVs da origem, tipa as colunas e reaproveita os mesmos
    `MODEL_BUILDERS` de `spark_jobs/silver_to_gold.py` — é o modelo dimensional
    **real**, materializado localmente (sem MinIO/Airflow).

    ```python title="scripts/exportar_gold.py"
    --8<-- "scripts/exportar_gold.py:55:78"
    ```

## 2. Importar no Looker Studio

1. Acesse <https://lookerstudio.google.com> (entra com conta Google comum).
2. **Criar → Fonte de dados → "Upload de arquivos (CSV)"** → envie `gold_export/obt_vendas.csv`.
3. **Criar relatório** a partir dessa fonte.

## 3. KPIs (cartões "Pontuação")

As 4 métricas, sobre a `obt_vendas`. Duas saem direto de agregações; duas são
**campos calculados**:

| KPI | Como |
|---|---|
| **Faturamento total** | `SUM(receita_liquida)` (formato Moeda) |
| **Qtd. de pedidos** | `COUNT_DISTINCT(id_pedido)` |
| **Ticket médio** | campo calculado abaixo |
| **% de pedidos entregues** | campo calculado abaixo |

```
Ticket Médio = SUM(receita_liquida) / COUNT_DISTINCT(id_pedido)
```

```
% Entregues = COUNT_DISTINCT(CASE WHEN status_entrega = "entregue" THEN id_pedido END)
            / COUNT_DISTINCT(CASE WHEN status_entrega != "" THEN id_pedido END)
```

!!! warning "Denominador do % Entregues"
    Use `status_entrega != ""` (e **não** `IS NOT NULL`): no CSV, células vazias
    chegam como **string vazia**, então `IS NOT NULL` contaria todos os pedidos.
    Com `!= ""` o índice considera só pedidos **com entrega registrada**.

## 4. Métricas e filtros

| Métrica | Como montar |
|---|---|
| **Faturamento por mês** | Gráfico de linhas: dimensão `ano_mes` · métrica `SUM(receita_liquida)` |
| **Produtos mais vendidos** | Gráfico de barras: dimensão `produto` · métrica `SUM(quantidade)` (Top 10) |

**Filtros** (controles → Lista suspensa): `ano_mes` (período) · `categoria` ·
`estado` · `forma_pagamento`. Eles filtram todos os visuais da página.

!!! tip "Valores de referência"
    Com os dados do projeto, o dashboard deve mostrar aproximadamente:
    **Faturamento R$ 4,76 mi · Pedidos 9.503 · Ticket R$ 501 · % Entregues 71,9%**.

## Referências

- [Looker Studio — Upload de arquivos (CSV)](https://support.google.com/looker-studio/answer/9971178)
- [Looker Studio — Campos calculados](https://support.google.com/looker-studio/answer/6299685)
- Modelo dimensional: [DAG Silver → Gold](dag_silver_gold.md)
- Página completa de [referências](referencias.md)
