"""Build and synchronize Gold dimensions and facts from Silver Delta tables."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from functools import reduce
from pathlib import Path
from typing import Any, Callable


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dags.lib.silver_gold import (  # noqa: E402
    GOLD_MODELS,
    SILVER_TABLES,
    build_manifest,
    build_manifest_key,
    build_spark_conf,
    gold_table_uri,
    parse_gold_models,
    parse_logical_date,
    silver_table_uri,
)


LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Gera o modelo dimensional Gold a partir da Silver."
    )
    parser.add_argument("--bucket", default="datalake")
    parser.add_argument("--database", default="ecommerce")
    parser.add_argument("--models", default=None)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--logical-date", required=True)
    parser.add_argument("--endpoint-url", required=True)
    return parser.parse_args()


def create_spark_session(endpoint_url: str):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("silver-to-gold")
    for key, value in build_spark_conf(endpoint_url).items():
        builder = builder.config(key, value)
    spark = builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def _date_key(column):
    from pyspark.sql import functions as F

    return F.date_format(column, "yyyyMMdd").cast("int")


def _build_dim_tempo(spark, silver):
    from pyspark.sql import functions as F

    date_columns = (
        ("clientes", "data_cadastro"),
        ("cupons", "data_validade"),
        ("pedidos", "data_pedido"),
        ("pagamentos", "data_pagamento"),
        ("entregas", "data_envio"),
        ("entregas", "data_entrega_prevista"),
        ("entregas", "data_entrega_real"),
        ("avaliacoes", "data_avaliacao"),
    )
    dates = [
        silver[table].select(F.col(column).cast("date").alias("data"))
        for table, column in date_columns
    ]
    all_dates = reduce(lambda left, right: left.unionByName(right), dates)
    bounds = all_dates.filter(F.col("data").isNotNull()).agg(
        F.min("data").alias("data_minima"),
        F.max("data").alias("data_maxima"),
    )
    calendar = bounds.select(
        F.explode(
            F.sequence(
                F.col("data_minima"),
                F.col("data_maxima"),
                F.expr("interval 1 day"),
            )
        ).alias("data")
    )
    month_names = F.array(
        *[
            F.lit(name)
            for name in (
                "Janeiro",
                "Fevereiro",
                "Março",
                "Abril",
                "Maio",
                "Junho",
                "Julho",
                "Agosto",
                "Setembro",
                "Outubro",
                "Novembro",
                "Dezembro",
            )
        ]
    )
    weekday_names = F.array(
        *[
            F.lit(name)
            for name in (
                "Domingo",
                "Segunda-feira",
                "Terça-feira",
                "Quarta-feira",
                "Quinta-feira",
                "Sexta-feira",
                "Sábado",
            )
        ]
    )
    return calendar.select(
        _date_key(F.col("data")).alias("data_key"),
        "data",
        F.year("data").alias("ano"),
        F.when(F.month("data") <= 6, 1).otherwise(2).alias("semestre"),
        F.quarter("data").alias("trimestre"),
        F.month("data").alias("mes"),
        F.element_at(month_names, F.month("data")).alias("mes_nome"),
        F.date_format("data", "yyyy-MM").alias("ano_mes"),
        F.weekofyear("data").alias("semana_ano"),
        F.dayofmonth("data").alias("dia_mes"),
        F.dayofweek("data").alias("dia_semana_numero"),
        F.element_at(weekday_names, F.dayofweek("data")).alias("dia_semana_nome"),
        F.dayofweek("data").isin(1, 7).alias("fim_de_semana"),
    )


def _build_dim_cliente(spark, silver):
    from pyspark.sql import functions as F

    return silver["clientes"].select(
        F.col("id_cliente").alias("cliente_key"),
        "id_cliente",
        "nome",
        "genero",
        "data_nascimento",
        "cidade",
        "estado",
        "data_cadastro",
    )


def _build_dim_produto(spark, silver):
    from pyspark.sql import functions as F

    produtos = silver["produtos"].alias("produto")
    categorias = silver["categorias"].alias("categoria")
    fornecedores = silver["fornecedores"].alias("fornecedor")
    return (
        produtos.join(
            categorias,
            F.col("produto.id_categoria") == F.col("categoria.id_categoria"),
            "inner",
        )
        .join(
            fornecedores,
            F.col("produto.id_fornecedor") == F.col("fornecedor.id_fornecedor"),
            "inner",
        )
        .select(
            F.col("produto.id_produto").alias("produto_key"),
            F.col("produto.id_produto"),
            F.col("produto.nome_produto"),
            F.col("produto.descricao").alias("descricao_produto"),
            F.col("produto.marca"),
            F.col("produto.preco"),
            F.col("produto.estoque"),
            F.col("produto.peso_kg"),
            F.col("categoria.id_categoria").alias("categoria_key"),
            F.col("categoria.nome_categoria"),
            F.col("categoria.descricao").alias("descricao_categoria"),
            F.col("fornecedor.id_fornecedor").alias("fornecedor_key"),
            F.col("fornecedor.nome_fornecedor"),
            F.col("fornecedor.estado").alias("estado_fornecedor"),
        )
    )


def _build_dim_cupom(spark, silver):
    from pyspark.sql import functions as F

    return silver["cupons"].select(
        F.col("id_cupom").alias("cupom_key"),
        "id_cupom",
        "codigo",
        "desconto_percentual",
        "valor_minimo",
        "data_validade",
        "ativo",
    )


def _build_fato_vendas(spark, silver):
    from pyspark.sql import functions as F

    itens = silver["itens_pedido"].alias("item")
    pedidos = silver["pedidos"].alias("pedido")
    joined = itens.join(
        pedidos,
        F.col("item.id_pedido") == F.col("pedido.id_pedido"),
        "inner",
    )
    valor_bruto = (F.col("item.quantidade") * F.col("item.valor_unitario")).cast(
        "decimal(18,2)"
    )
    return joined.select(
        F.col("item.id_item").alias("venda_key"),
        F.col("item.id_item"),
        F.col("pedido.id_pedido"),
        _date_key(F.col("pedido.data_pedido")).alias("data_key"),
        F.year("pedido.data_pedido").alias("ano"),
        F.col("pedido.id_cliente").alias("cliente_key"),
        F.col("item.id_produto").alias("produto_key"),
        F.col("pedido.id_cupom").alias("cupom_key"),
        F.col("pedido.status").alias("status_pedido"),
        F.col("item.quantidade"),
        F.col("item.valor_unitario"),
        F.col("item.desconto_percentual"),
        valor_bruto.alias("valor_bruto"),
        (valor_bruto - F.col("item.subtotal"))
        .cast("decimal(18,2)")
        .alias("valor_desconto"),
        F.col("item.subtotal").alias("receita_liquida"),
        F.lit(1).alias("quantidade_itens_registro"),
    )


def _build_fato_pagamentos(spark, silver):
    from pyspark.sql import functions as F

    pagamentos = silver["pagamentos"].alias("pagamento")
    pedidos = silver["pedidos"].alias("pedido")
    return pagamentos.join(
        pedidos,
        F.col("pagamento.id_pedido") == F.col("pedido.id_pedido"),
        "inner",
    ).select(
        F.col("pagamento.id_pagamento").alias("pagamento_key"),
        F.col("pagamento.id_pagamento"),
        F.col("pedido.id_pedido"),
        _date_key(F.col("pagamento.data_pagamento")).alias("data_key"),
        F.year("pagamento.data_pagamento").alias("ano"),
        F.col("pedido.id_cliente").alias("cliente_key"),
        F.col("pagamento.forma_pagamento"),
        F.col("pagamento.status_pagamento"),
        F.col("pagamento.valor"),
        F.when(
            F.col("pagamento.status_pagamento") == "aprovado",
            F.col("pagamento.valor"),
        )
        .otherwise(F.lit(0).cast("decimal(18,2)"))
        .alias("valor_aprovado"),
        F.col("pagamento.parcelas"),
        F.lit(1).alias("quantidade_pagamentos"),
    )


def _build_fato_entregas(spark, silver):
    from pyspark.sql import functions as F

    entregas = silver["entregas"].alias("entrega")
    pedidos = silver["pedidos"].alias("pedido")
    data_real = F.col("entrega.data_entrega_real")
    data_prevista = F.col("entrega.data_entrega_prevista")
    return entregas.join(
        pedidos,
        F.col("entrega.id_pedido") == F.col("pedido.id_pedido"),
        "inner",
    ).select(
        F.col("entrega.id_entrega").alias("entrega_key"),
        F.col("entrega.id_entrega"),
        F.col("pedido.id_pedido"),
        _date_key(F.col("entrega.data_envio")).alias("data_envio_key"),
        _date_key(data_prevista).alias("data_prevista_key"),
        _date_key(data_real).alias("data_real_key"),
        F.year("entrega.data_envio").alias("ano"),
        F.col("pedido.id_cliente").alias("cliente_key"),
        F.col("entrega.status_entrega"),
        F.col("entrega.transportadora"),
        F.col("entrega.codigo_rastreio"),
        F.datediff(data_prevista, F.col("entrega.data_envio")).alias(
            "prazo_previsto_dias"
        ),
        F.when(
            data_real.isNotNull(),
            F.datediff(data_real, F.col("entrega.data_envio")),
        ).alias("prazo_real_dias"),
        F.when(
            data_real.isNotNull(),
            F.greatest(F.datediff(data_real, data_prevista), F.lit(0)),
        ).alias("atraso_dias"),
        F.when(data_real.isNotNull(), data_real <= data_prevista).alias(
            "entrega_no_prazo"
        ),
        F.lit(1).alias("quantidade_entregas"),
    )


def _build_fato_avaliacoes(spark, silver):
    from pyspark.sql import functions as F

    return silver["avaliacoes"].select(
        F.col("id_avaliacao").alias("avaliacao_key"),
        "id_avaliacao",
        "id_pedido",
        _date_key(F.col("data_avaliacao")).alias("data_key"),
        F.year("data_avaliacao").alias("ano"),
        F.col("id_cliente").alias("cliente_key"),
        F.col("id_produto").alias("produto_key"),
        "nota",
        "comentario",
        (F.col("nota") >= 4).alias("avaliacao_positiva"),
        F.lit(1).alias("quantidade_avaliacoes"),
    )


MODEL_BUILDERS: dict[str, Callable] = {
    "dim_tempo": _build_dim_tempo,
    "dim_cliente": _build_dim_cliente,
    "dim_produto": _build_dim_produto,
    "dim_cupom": _build_dim_cupom,
    "fato_vendas": _build_fato_vendas,
    "fato_pagamentos": _build_fato_pagamentos,
    "fato_entregas": _build_fato_entregas,
    "fato_avaliacoes": _build_fato_avaliacoes,
}


def _add_metadata(data, *, run_id: str):
    from pyspark.sql import functions as F

    business_columns = data.columns
    return (
        data.withColumn(
            "_gold_record_hash",
            F.sha2(F.to_json(F.struct(*business_columns)), 256),
        )
        .withColumn("_gold_airflow_run_id", F.lit(run_id))
        .withColumn("_gold_processed_at", F.current_timestamp())
    )


def _sync_gold_table(
    spark,
    data,
    *,
    output_uri: str,
    primary_key: str,
    partition_columns: tuple[str, ...],
) -> dict[str, int]:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    source_count = data.count()
    if not source_count:
        raise ValueError(f"Gold model {output_uri!r} produced no records")

    if not DeltaTable.isDeltaTable(spark, output_uri):
        writer = data.write.format("delta").mode("append")
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.save(output_uri)
        return {
            "inserted": source_count,
            "updated": 0,
            "deleted": 0,
            "unchanged": 0,
            "rows_written": source_count,
        }

    target = (
        spark.read.format("delta")
        .load(output_uri)
        .select(
            F.col(primary_key).alias("_target_primary_key"),
            F.col("_gold_record_hash").alias("_target_record_hash"),
        )
    )
    comparison = data.join(
        target,
        data[primary_key] == target["_target_primary_key"],
        "left",
    )
    inserted = comparison.filter(F.col("_target_primary_key").isNull()).count()
    updated = comparison.filter(
        F.col("_target_primary_key").isNotNull()
        & (F.col("_gold_record_hash") != F.col("_target_record_hash"))
    ).count()
    deleted = target.join(
        data.select(F.col(primary_key).alias("_target_primary_key")),
        on="_target_primary_key",
        how="left_anti",
    ).count()

    if inserted or updated or deleted:
        (
            DeltaTable.forPath(spark, output_uri)
            .alias("target")
            .merge(
                data.alias("source"),
                f"target.{primary_key} = source.{primary_key}",
            )
            .whenMatchedUpdateAll(
                condition=("source._gold_record_hash <> target._gold_record_hash")
            )
            .whenNotMatchedInsertAll()
            .whenNotMatchedBySourceDelete()
            .execute()
        )

    return {
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "unchanged": source_count - inserted - updated,
        "rows_written": inserted + updated + deleted,
    }


def process_model(
    spark,
    silver,
    *,
    bucket: str,
    database: str,
    model: str,
    run_id: str,
) -> dict[str, Any]:
    from pyspark.storagelevel import StorageLevel

    rule = GOLD_MODELS[model]
    output_uri = gold_table_uri(bucket, database, model)
    model_data = _add_metadata(
        MODEL_BUILDERS[model](spark, silver),
        run_id=run_id,
    )
    model_data.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        records_modelled = model_data.count()
        metrics = _sync_gold_table(
            spark,
            model_data,
            output_uri=output_uri,
            primary_key=rule.primary_key,
            partition_columns=rule.partition_columns,
        )
    finally:
        model_data.unpersist()

    result = {
        "table": model,
        "kind": rule.kind,
        "primary_key": rule.primary_key,
        "source_tables": list(rule.source_tables),
        "output_uri": output_uri,
        "partition_columns": list(rule.partition_columns),
        "records_modelled": records_modelled,
        **metrics,
        "status": "written" if metrics["rows_written"] else "already_current",
    }
    LOGGER.info("Gold model processed: %s", result)
    return result


def write_manifest(
    spark,
    *,
    bucket: str,
    run_id: str,
    logical_date: datetime,
    database: str,
    results: list[dict[str, Any]],
) -> str:
    key = build_manifest_key(logical_date, run_id)
    body = build_manifest(
        run_id=run_id,
        logical_date=logical_date,
        database=database,
        results=results,
    )
    uri = f"s3a://{bucket}/{key}"
    path = spark._jvm.org.apache.hadoop.fs.Path(uri)
    file_system = path.getFileSystem(spark._jsc.hadoopConfiguration())
    output = file_system.create(path, True)
    try:
        output.write(bytearray(body.encode("utf-8")))
    finally:
        output.close()
    return key


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logical_date = parse_logical_date(args.logical_date)
    models = parse_gold_models(args.models)
    required_silver = {
        source for model in models for source in GOLD_MODELS[model].source_tables
    }
    spark = create_spark_session(args.endpoint_url)
    silver = {
        table: spark.read.format("delta").load(
            silver_table_uri(args.bucket, args.database, table)
        )
        for table in SILVER_TABLES
        if table in required_silver
    }

    try:
        results = [
            process_model(
                spark,
                silver,
                bucket=args.bucket,
                database=args.database,
                model=model,
                run_id=args.run_id,
            )
            for model in models
        ]
        manifest_key = write_manifest(
            spark,
            bucket=args.bucket,
            run_id=args.run_id,
            logical_date=logical_date,
            database=args.database,
            results=results,
        )
        summary = {
            "manifest_key": manifest_key,
            "rows_written": sum(item["rows_written"] for item in results),
            "results": results,
        }
        LOGGER.info(
            "Silver to Gold completed: rows=%s manifest=s3://%s/%s",
            summary["rows_written"],
            args.bucket,
            manifest_key,
        )
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
