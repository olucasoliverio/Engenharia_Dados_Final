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
    SCD2_BEGINNING_OF_TIME,
    SCD2_END_OF_TIME,
    SCD2_IS_CURRENT,
    SCD2_RECORD_HASH,
    SCD2_VALID_FROM,
    SCD2_VALID_TO,
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
        F.col("pedido.data_pedido"),
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
        F.col("pagamento.data_pagamento"),
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
        F.col("entrega.data_envio"),
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
        "data_avaliacao",
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


# Para cada fato: quais dimensões SCD2 anexar e a data do evento usada no
# join point-in-time (a surrogate vigente naquela data).
FACT_DIMENSION_LINKS: dict[str, tuple[tuple[str, str], ...]] = {
    "fato_vendas": (
        ("dim_cliente", "data_pedido"),
        ("dim_produto", "data_pedido"),
        ("dim_cupom", "data_pedido"),
    ),
    "fato_pagamentos": (("dim_cliente", "data_pagamento"),),
    "fato_entregas": (("dim_cliente", "data_envio"),),
    "fato_avaliacoes": (
        ("dim_cliente", "data_avaliacao"),
        ("dim_produto", "data_avaliacao"),
    ),
}

# Colunas de metadado/controle que não entram no hash de negócio.
_NON_BUSINESS_COLUMNS = {
    "_gold_record_hash",
    "_gold_airflow_run_id",
    "_gold_processed_at",
    SCD2_VALID_FROM,
    SCD2_VALID_TO,
    SCD2_IS_CURRENT,
    SCD2_RECORD_HASH,
}


def _with_run_metadata(data, *, run_id: str):
    from pyspark.sql import functions as F

    return data.withColumn("_gold_airflow_run_id", F.lit(run_id)).withColumn(
        "_gold_processed_at", F.current_timestamp()
    )


def _add_type1_metadata(data, *, run_id: str):
    """Hash sobre todas as colunas de negócio (modelos Tipo 1 / estáticos)."""
    from pyspark.sql import functions as F

    business_columns = [c for c in data.columns if c not in _NON_BUSINESS_COLUMNS]
    hashed = data.withColumn(
        "_gold_record_hash",
        F.sha2(F.to_json(F.struct(*business_columns)), 256),
    )
    return _with_run_metadata(hashed, run_id=run_id)


def _add_scd2_attributes_hash(data, *, natural_key: str):
    """Hash dos atributos versionados (exclui a chave natural e os controles)."""
    from pyspark.sql import functions as F

    attribute_columns = [
        c
        for c in data.columns
        if c != natural_key and c not in _NON_BUSINESS_COLUMNS
    ]
    return data.withColumn(
        SCD2_RECORD_HASH,
        F.sha2(F.to_json(F.struct(*attribute_columns)), 256),
    )


def _surrogate_key(natural_key: str, valid_from):
    """Surrogate determinístico e único por versão (chave natural + vigência)."""
    from pyspark.sql import functions as F

    return F.sha2(
        F.concat_ws(
            "||",
            F.col(natural_key).cast("string"),
            F.date_format(valid_from, "yyyy-MM-dd HH:mm:ss.SSS"),
        ),
        256,
    )


def _load_gold_dim(spark, *, bucket: str, database: str, model: str):
    return spark.read.format("delta").load(gold_table_uri(bucket, database, model))


def _attach_dimension_sk(fact, dim, *, natural_key: str, surrogate_key: str, event_date_col: str):
    """Anexa a surrogate vigente da dimensão na data do evento (point-in-time)."""
    from pyspark.sql import functions as F

    versions = dim.select(
        F.col(natural_key).alias("_pit_nk"),
        F.col(surrogate_key),
        F.col(SCD2_VALID_FROM).alias("_pit_from"),
        F.col(SCD2_VALID_TO).alias("_pit_to"),
    )
    event_ts = F.col(event_date_col).cast("timestamp")
    condition = (
        (F.col(natural_key) == F.col("_pit_nk"))
        & (event_ts >= F.col("_pit_from"))
        & (event_ts < F.col("_pit_to"))
    )
    return fact.join(versions, condition, "left").drop(
        "_pit_nk", "_pit_from", "_pit_to"
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


def _sync_dimension_scd2(
    spark,
    source,
    *,
    output_uri: str,
    natural_key: str,
    surrogate_key: str,
    run_id: str,
) -> dict[str, int]:
    """Carga SCD Tipo 2: preserva histórico versionando atributos alterados.

    A *source* já deve conter a chave natural, os atributos e o
    ``dw_record_hash``. Na primeira carga cada chave entra como versão vigente
    desde ``SCD2_BEGINNING_OF_TIME``. Em cargas seguintes, chaves alteradas têm
    a versão anterior expirada e uma nova versão vigente inserida (staged merge
    do Delta); chaves inalteradas não geram escrita.
    """
    from datetime import datetime, timezone

    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    source_count = source.count()
    if not source_count:
        raise ValueError(f"Gold dimension {output_uri!r} produced no records")

    low = F.to_timestamp(F.lit(SCD2_BEGINNING_OF_TIME))
    high = F.to_timestamp(F.lit(SCD2_END_OF_TIME))

    def _finalize_new_versions(data, valid_from_col):
        prepared = (
            data.withColumn(SCD2_VALID_FROM, valid_from_col)
            .withColumn(SCD2_VALID_TO, high)
            .withColumn(SCD2_IS_CURRENT, F.lit(True))
        )
        prepared = prepared.withColumn(
            surrogate_key, _surrogate_key(natural_key, F.col(SCD2_VALID_FROM))
        )
        return _with_run_metadata(prepared, run_id=run_id)

    if not DeltaTable.isDeltaTable(spark, output_uri):
        initial = _finalize_new_versions(source, low)
        initial.write.format("delta").save(output_uri)
        return {
            "inserted": source_count,
            "updated": 0,
            "deleted": 0,
            "unchanged": 0,
            "versions_expired": 0,
            "versions_inserted": source_count,
            "rows_written": source_count,
        }

    current = (
        spark.read.format("delta")
        .load(output_uri)
        .filter(F.col(SCD2_IS_CURRENT))
        .select(
            F.col(natural_key).alias("_cur_nk"),
            F.col(SCD2_RECORD_HASH).alias("_cur_hash"),
        )
    )
    joined = source.join(
        current, source[natural_key] == current["_cur_nk"], "left"
    )
    is_new = F.col("_cur_nk").isNull()
    is_changed = (~is_new) & (F.col(SCD2_RECORD_HASH) != F.col("_cur_hash"))

    flagged = joined.withColumn(
        "_scd_action",
        F.when(is_new, F.lit("new"))
        .when(is_changed, F.lit("changed"))
        .otherwise(F.lit("unchanged")),
    ).drop("_cur_nk", "_cur_hash")

    new_count = flagged.filter(F.col("_scd_action") == "new").count()
    changed_count = flagged.filter(F.col("_scd_action") == "changed").count()
    unchanged_count = source_count - new_count - changed_count

    if not new_count and not changed_count:
        return {
            "inserted": 0,
            "updated": 0,
            "deleted": 0,
            "unchanged": unchanged_count,
            "versions_expired": 0,
            "versions_inserted": 0,
            "rows_written": 0,
        }

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    change_ts = F.to_timestamp(F.lit(now_str))

    candidates = flagged.filter(F.col("_scd_action") != "unchanged")
    new_versions = _finalize_new_versions(
        candidates,
        F.when(F.col("_scd_action") == "new", low).otherwise(change_ts),
    )

    target_columns = spark.read.format("delta").load(output_uri).columns
    insert_payload = new_versions.drop("_scd_action").withColumn(
        "_merge_key", F.lit(None).cast("string")
    )
    expire_payload = (
        new_versions.filter(F.col("_scd_action") == "changed")
        .drop("_scd_action")
        .withColumn("_merge_key", F.col(natural_key).cast("string"))
    )
    staged = insert_payload.unionByName(expire_payload)

    (
        DeltaTable.forPath(spark, output_uri)
        .alias("target")
        .merge(staged.alias("source"), f"target.{natural_key} = source._merge_key")
        .whenMatchedUpdate(
            condition=f"target.{SCD2_IS_CURRENT} = true AND source._merge_key IS NOT NULL",
            set={
                SCD2_VALID_TO: f"source.{SCD2_VALID_FROM}",
                SCD2_IS_CURRENT: F.lit(False),
            },
        )
        .whenNotMatchedInsert(
            condition="source._merge_key IS NULL",
            values={col: f"source.{col}" for col in target_columns},
        )
        .execute()
    )

    return {
        "inserted": new_count,
        "updated": changed_count,
        "deleted": 0,
        "unchanged": unchanged_count,
        "versions_expired": changed_count,
        "versions_inserted": new_count + changed_count,
        "rows_written": new_count + changed_count * 2,
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
    base = MODEL_BUILDERS[model](spark, silver)

    if rule.scd_type == "type2":
        model_data = _add_scd2_attributes_hash(base, natural_key=rule.natural_key)
    else:
        enriched = base
        for dim_model, event_col in FACT_DIMENSION_LINKS.get(model, ()):
            dim_rule = GOLD_MODELS[dim_model]
            dim_df = _load_gold_dim(
                spark, bucket=bucket, database=database, model=dim_model
            )
            enriched = _attach_dimension_sk(
                enriched,
                dim_df,
                natural_key=dim_rule.natural_key,
                surrogate_key=dim_rule.surrogate_key,
                event_date_col=event_col,
            )
        model_data = _add_type1_metadata(enriched, run_id=run_id)

    model_data.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        records_modelled = model_data.count()
        if rule.scd_type == "type2":
            metrics = _sync_dimension_scd2(
                spark,
                model_data,
                output_uri=output_uri,
                natural_key=rule.natural_key,
                surrogate_key=rule.surrogate_key,
                run_id=run_id,
            )
        else:
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
