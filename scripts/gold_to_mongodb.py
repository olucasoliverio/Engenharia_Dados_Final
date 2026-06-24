import os
from datetime import date, datetime
from decimal import Decimal
from pyspark.sql import SparkSession
from pymongo import MongoClient

# ── Configurações ──────────────────────────────────────────────
MINIO_ENDPOINT  = os.getenv("SPARK_S3_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS    = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
MINIO_SECRET    = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
BUCKET          = os.getenv("LANDING_BUCKET", "datalake")

MONGO_URI       = os.getenv("MONGO_URI", "mongodb://admin:admin123@localhost:27017/?authSource=admin")
MONGO_DB        = os.getenv("MONGO_DB", "ecommerce")

TABELAS_GOLD = [
    "fato_vendas",
    "fato_pagamentos",
    "fato_entregas",
    "fato_avaliacoes",
    "dim_cliente",
    "dim_produto",
    "dim_tempo",
    "dim_cupom",
]

# ── Spark ──────────────────────────────────────────────────────
def criar_spark():
    return (
        SparkSession.builder
        .appName("gold_to_mongodb")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.2.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint",          MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        MINIO_ACCESS)
        .config("spark.hadoop.fs.s3a.secret.key",        MINIO_SECRET)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

# ── MongoDB ────────────────────────────────────────────────────
def salvar_no_mongo(db, nome_colecao, registros):
    colecao = db[nome_colecao]
    colecao.drop()                    # limpa antes de reinserir
    if registros:
        colecao.insert_many(registros)
    print(f"  → {len(registros)} documentos salvos em '{nome_colecao}'")

# ── Main ───────────────────────────────────────────────────────
def main():
    print("Iniciando exportação Gold → MongoDB...\n")

    spark  = criar_spark()
    client = MongoClient(MONGO_URI)
    db     = client[MONGO_DB]

    for tabela in TABELAS_GOLD:
        caminho = f"s3a://{BUCKET}/gold/ecommerce/{tabela}/"
        print(f"Lendo: {caminho}")

        try:
            df = spark.read.format("delta").load(caminho)
            # converte para lista de dicts (remove campo _id do Spark se existir
            # e converte datetime.date e decimal.Decimal para tipos que o BSON aceita)
            registros = [
                {
                    k: (
                        datetime.combine(v, datetime.min.time())
                        if isinstance(v, date) and not isinstance(v, datetime)
                        else float(v)
                        if isinstance(v, Decimal)
                        else v
                    )
                    for k, v in row.asDict().items()
                    if k != "_id"
                }
                for row in df.collect()
            ]
            salvar_no_mongo(db, f"gold_{tabela}", registros)

        except Exception as e:
            print(f"  [ERRO] {tabela}: {e}")

    client.close()
    spark.stop()
    print("\nExportação concluída!")
    print(f"Coleções criadas no banco '{MONGO_DB}': gold_<tabela>")
    print("Agora conecte o Power BI no MongoDB usando essas coleções.")

if __name__ == "__main__":
    main()