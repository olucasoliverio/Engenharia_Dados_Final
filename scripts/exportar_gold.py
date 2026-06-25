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


def build_obt_vendas(spark, silver):
    """OBT achatada (1 linha por item de pedido) com tudo que o dashboard precisa.

    Junta itens + pedido + cliente + produto/categoria/fornecedor + cupom e traz
    1 pagamento e 1 entrega por pedido (dedup) para evitar fan-out. Grão = item.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    def primeiro_por_pedido(df, ordem):
        w = Window.partitionBy("id_pedido").orderBy(ordem)
        return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

    pag = primeiro_por_pedido(silver["pagamentos"], "id_pagamento").alias("pg")
    ent = primeiro_por_pedido(silver["entregas"], "id_entrega").alias("e")
    i = silver["itens_pedido"].alias("i")
    p = silver["pedidos"].alias("p")
    c = silver["clientes"].alias("c")
    pr = silver["produtos"].alias("pr")
    cat = silver["categorias"].alias("cat")
    f = silver["fornecedores"].alias("f")
    cup = silver["cupons"].alias("cup")

    valor_bruto = (F.col("i.quantidade") * F.col("i.valor_unitario")).cast("decimal(18,2)")
    data_real = F.col("e.data_entrega_real")
    data_prev = F.col("e.data_entrega_prevista")

    return (
        i.join(p, F.col("i.id_pedido") == F.col("p.id_pedido"), "inner")
        .join(c, F.col("p.id_cliente") == F.col("c.id_cliente"), "left")
        .join(pr, F.col("i.id_produto") == F.col("pr.id_produto"), "left")
        .join(cat, F.col("pr.id_categoria") == F.col("cat.id_categoria"), "left")
        .join(f, F.col("pr.id_fornecedor") == F.col("f.id_fornecedor"), "left")
        .join(cup, F.col("p.id_cupom") == F.col("cup.id_cupom"), "left")
        .join(pag, F.col("p.id_pedido") == F.col("pg.id_pedido"), "left")
        .join(ent, F.col("p.id_pedido") == F.col("e.id_pedido"), "left")
        .select(
            F.col("i.id_item"),
            F.col("p.id_pedido"),
            F.col("p.data_pedido"),
            F.year("p.data_pedido").alias("ano"),
            F.date_format("p.data_pedido", "yyyy-MM").alias("ano_mes"),
            F.col("p.status").alias("status_pedido"),
            F.col("c.nome").alias("cliente"),
            F.col("c.cidade"),
            F.col("c.estado"),
            F.col("pr.nome_produto").alias("produto"),
            F.col("cat.nome_categoria").alias("categoria"),
            F.col("pr.marca"),
            F.col("cup.codigo").alias("cupom"),
            F.col("i.quantidade"),
            F.col("i.valor_unitario"),
            valor_bruto.alias("valor_bruto"),
            (valor_bruto - F.col("i.subtotal")).cast("decimal(18,2)").alias("valor_desconto"),
            F.col("i.subtotal").alias("receita_liquida"),
            F.col("pg.forma_pagamento"),
            F.col("pg.status_pagamento"),
            F.col("e.status_entrega"),
            F.when(data_real.isNotNull(), data_real <= data_prev).alias("entrega_no_prazo"),
        )
    )


def _escrever_csv(df, nome: str) -> Path:
    """Grava um CSV unico (renomeando o part-file do Spark)."""
    tmp = OUT_DIR / f"_{nome}_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(tmp))
    part = next(tmp.glob("part-*.csv"))
    destino = OUT_DIR / f"{nome}.csv"
    destino.unlink(missing_ok=True)
    part.rename(destino)
    shutil.rmtree(tmp)
    return destino


def main() -> int:
    spark = criar_spark()
    spark.sparkContext.setLogLevel("ERROR")
    silver = ler_origem_tipada(spark)
    OUT_DIR.mkdir(exist_ok=True)

    total = 0
    # Modelo estrela: 1 CSV por dimensao/fato.
    for model in GOLD_MODELS:
        df = MODEL_BUILDERS[model](spark, silver)
        linhas = df.count()
        _escrever_csv(df, model)
        print(f"  ok {model}: {linhas:,} linhas")
        total += linhas

    # OBT achatada (1 tabela so) para o caminho simples no Power BI.
    obt = build_obt_vendas(spark, silver)
    linhas_obt = obt.count()
    _escrever_csv(obt, "obt_vendas")
    print(f"  ok obt_vendas: {linhas_obt:,} linhas")

    print(f"\nConcluido em {OUT_DIR}/ : estrela ({total:,} linhas em "
          f"{len(GOLD_MODELS)} tabelas) + obt_vendas ({linhas_obt:,} linhas).")
    print("Power BI simples: importe apenas obt_vendas.csv.")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
