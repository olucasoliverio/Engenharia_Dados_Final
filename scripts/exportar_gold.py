"""Exporta o modelo dimensional Gold para CSV (consumo em BI / Power BI).

Reconstrói as 4 dimensões + 4 fatos a partir dos CSVs da origem
(`dataset/arquivos_csv/`), reaproveitando os mesmos builders de
`spark_jobs/silver_to_gold.py`, e grava **um CSV por tabela** em `gold_export/`.

É um atalho de visualização: não depende de MinIO/Airflow. Os fatos referenciam
as dimensões pelas chaves naturais (`cliente_key`, `produto_key`, `cupom_key`,
`data_key`) — exatamente o que o Power BI usa para montar o esquema estrela.

Uso:
    python scripts/exportar_gold.py            # gera gold_export/<tabela>.csv

Requisitos: PySpark (grupo `[spark]` do pyproject).
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from dataset.scripts_py.carregar_mongo import COLECOES  # noqa: E402  (mapa de tipos)
from dags.lib.silver_gold import GOLD_MODELS  # noqa: E402
from spark_jobs.silver_to_gold import MODEL_BUILDERS  # noqa: E402

CSV_DIR = REPO_ROOT / "dataset" / "arquivos_csv"
OUT_DIR = REPO_ROOT / "gold_export"


def criar_spark():
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName("exportar-gold")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def ler_origem_tipada(spark) -> dict:
    """Lê os CSVs da origem e tipa as colunas (faz o papel da Silver para o BI)."""
    from pyspark.sql import functions as F

    silver = {}
    for nome, tipos in COLECOES.items():
        csv_path = CSV_DIR / tipos["csv"]
        if not csv_path.exists():
            raise SystemExit(
                f"CSV ausente: {csv_path}\n"
                "Gere os dados antes: python dataset/scripts_py/gerar_dados.py"
            )
        df = spark.read.option("header", True).csv(str(csv_path))
        for col in tipos["int"]:
            df = df.withColumn(col, F.col(col).cast("int"))
        for col in tipos["float"]:
            df = df.withColumn(col, F.col(col).cast("double"))
        for col in tipos["bool"]:
            df = df.withColumn(col, F.lower(F.col(col)).isin("true", "1", "sim"))
        for col in tipos["date"]:
            df = df.withColumn(col, F.to_timestamp(F.col(col)))
        silver[nome] = df
    return silver


def main() -> int:
    spark = criar_spark()
    spark.sparkContext.setLogLevel("ERROR")
    silver = ler_origem_tipada(spark)
    OUT_DIR.mkdir(exist_ok=True)

    total = 0
    for model in GOLD_MODELS:
        df = MODEL_BUILDERS[model](spark, silver)
        linhas = df.count()
        # Escreve um unico CSV com cabecalho e renomeia o part-file do Spark.
        tmp = OUT_DIR / f"_{model}_tmp"
        df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(tmp))
        part = next(tmp.glob("part-*.csv"))
        destino = OUT_DIR / f"{model}.csv"
        destino.unlink(missing_ok=True)
        part.rename(destino)
        shutil.rmtree(tmp)
        print(f"  ok {model}: {linhas:,} linhas -> {destino.name}")
        total += linhas

    print(f"\nConcluido: {total:,} linhas em {len(GOLD_MODELS)} tabelas em {OUT_DIR}/")
    print("Importe os CSVs no Power BI e ligue os fatos as dimensoes pelas chaves "
          "(cliente_key, produto_key, cupom_key, data_key).")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
