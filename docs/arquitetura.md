---
tags:
  - arquitetura
  - medalhão
---

# Arquitetura da solução

Visão geral de ponta a ponta: da origem **NoSQL** até o **modelo dimensional**
consumido pelo dashboard, seguindo a arquitetura **Medalhão** sobre um Data Lake.

## :material-sitemap: Fluxo completo

```mermaid
flowchart TB
    subgraph Origem
        M[("MongoDB / Atlas<br/>10 coleções")]
    end
    subgraph Orquestração
        AF["Apache Airflow<br/>4 DAGs"]
    end
    subgraph "Data Lake (MinIO · S3)"
        L["Landing<br/>JSON bruto"]
        B["Bronze<br/>Delta"]
        S["Silver<br/>Delta · Data Quality"]
        G[("Gold<br/>dimensional · SCD2")]
    end
    D[["Dashboard<br/>4 KPIs + 2 métricas"]]

    M --> AF
    AF -->|mongodb_to_landing| L
    AF -->|landing_to_bronze| B
    AF -->|bronze_to_silver| S
    AF -->|silver_to_gold| G
    L --> B --> S --> G --> D

    style M fill:#15803d,color:#fff,stroke:#166534
    style L fill:#e2e8f0,stroke:#94a3b8,color:#1e293b
    style B fill:#fdba74,stroke:#b45309,color:#7c2d12
    style S fill:#cbd5e1,stroke:#64748b,color:#1e293b
    style G fill:#fde047,stroke:#a16207,color:#713f12
    style D fill:#ede9fe,stroke:#7c3aed,color:#4c1d95
```

## :material-format-list-checks: Princípios de design

| Decisão | Escolha | Por quê |
|---|---|---|
| Origem | MongoDB (NoSQL) | Cenário realista de dados semiestruturados |
| Object storage | MinIO (S3-compatible) | Data Lake local, portável para a nuvem |
| Orquestração | Apache Airflow | Agendamento e dependências entre etapas (sem cron/Windows) |
| Formato analítico | Delta Lake | ACID, `MERGE`, time-travel e schema enforcement |
| Camadas | Landing · Bronze · Silver · Gold | Separação de responsabilidades (medalhão) |
| Modelagem Gold | Dimensional (Kimball) | Fatos e dimensões prontos para BI |
| Histórico | SCD Tipo 2 nas dimensões | Preserva versões dos atributos ao longo do tempo |
| Incremental | `updated_at` + checkpoints | Reprocessa apenas o que mudou |

## :material-layers-triple: Responsabilidade de cada camada

- <span class="accent-landing">**Landing**</span> — cópia fiel da origem em **JSON**
  (Extended JSON), sem transformação. [:octicons-arrow-right-24: detalhes](estrutura_landing.md)
- <span class="accent-bronze">**Bronze**</span> — JSON → **Delta**, com metadados de
  auditoria e idempotência por arquivo. [:octicons-arrow-right-24: detalhes](estrutura_bronze.md)
- <span class="accent-silver">**Silver**</span> — limpeza, tipagem, validações e
  integridade referencial. [:octicons-arrow-right-24: detalhes](estrutura_silver.md)
- <span class="accent-gold">**Gold**</span> — modelo dimensional (4 dimensões + 4
  fatos), SCD Tipo 2. [:octicons-arrow-right-24: detalhes](estrutura_gold.md)

## :material-image-multiple: Diagramas

Diagramas de arquitetura versionados no repositório (em `assets/`):

- [Arquitetura do projeto](https://github.com/olucasoliverio/Engenharia_Dados_Final/blob/main/assets/architecture_project.jpg)
- [Arquitetura — MongoDB](https://github.com/olucasoliverio/Engenharia_Dados_Final/blob/main/assets/architecture_mongoDB.jpg)
- [Board no Miro](https://miro.com/app/board/uXjVHEnXdmQ=/)

## Referências

- [The Data Engineering Cookbook — Medallion Architecture](https://docs.databricks.com/lakehouse/medallion.html)
- [Delta Lake](https://docs.delta.io/latest/index.html)
- [Apache Airflow — Concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/index.html)
- Página completa de [referências](referencias.md)
