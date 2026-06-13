# Engenharia de Dados — Projeto Final

Este projeto implementa um pipeline de dados para um e-commerce fictício,
partindo de uma origem MongoDB e seguindo a arquitetura medalhão no Data Lake.

## Fluxo

```mermaid
flowchart LR
    A[MongoDB Atlas] --> B[Apache Airflow]
    B --> C[Landing JSON]
    C --> D[Bronze Delta]
    D --> E[Silver Delta]
    E --> F[Gold dimensional]
    F --> G[Dashboard]
```

## Conteúdo disponível

- [Modelo MongoDB](modelo_mongodb.md): coleções, tipos e relacionamentos.
- [MongoDB Atlas](mongodb_atlas.md): configuração da origem compartilhada.
- [Estrutura Landing](estrutura_landing.md): MinIO, bucket, prefixos e
  validação.
- [DAG MongoDB → Landing](dag_mongodb_landing.md): ingestão incremental,
  checkpoints e formato dos arquivos.
