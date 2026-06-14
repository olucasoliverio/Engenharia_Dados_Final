"""Clean and merge Bronze Delta records into typed Silver Delta tables."""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from dags.lib.bronze_silver import (  # noqa: E402
    ENTITY_RULES,
    EntityRule,
    FieldRule,
    bronze_table_uri,
    build_manifest,
    build_manifest_key,
    build_spark_conf,
    parse_logical_date,
    parse_pipeline_tables,
    silver_table_uri,
)


LOGGER = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Limpa tabelas Bronze e realiza merge Delta na Silver."
    )
    parser.add_argument("--bucket", default="datalake")
    parser.add_argument("--database", default="ecommerce")
    parser.add_argument("--tables", default=None)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--logical-date", required=True)
    parser.add_argument("--endpoint-url", required=True)
    return parser.parse_args()


def create_spark_session(endpoint_url: str):
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("bronze-to-silver")
    for key, value in build_spark_conf(endpoint_url).items():
        builder = builder.config(key, value)
    spark = builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def _source_schema(rule: EntityRule):
    from pyspark.sql.types import (
        BooleanType,
        StringType,
        StructField,
        StructType,
    )

    numeric = StructType(
        [
            StructField("$numberInt", StringType(), True),
            StructField("$numberLong", StringType(), True),
            StructField("$numberDouble", StringType(), True),
        ]
    )
    date = StructType(
        [
            StructField(
                "$date",
                StructType([StructField("$numberLong", StringType(), True)]),
                True,
            )
        ]
    )
    source_types = {
        "string": StringType(),
        "boolean": BooleanType(),
        "integer": numeric,
        "number": numeric,
        "date": date,
    }
    return StructType(
        [
            StructField(field.name, source_types[field.source_type], True)
            for field in rule.fields
        ]
    )


def _field_expression(document, field: FieldRule):
    from pyspark.sql import functions as F

    value = document.getField(field.name)
    if field.source_type in {"integer", "number"}:
        value = F.coalesce(
            value.getField("$numberInt"),
            value.getField("$numberLong"),
            value.getField("$numberDouble"),
        ).cast(field.target_type)
    elif field.source_type == "date":
        milliseconds = value.getField("$date").getField("$numberLong").cast("long")
        value = F.to_timestamp(F.from_unixtime(milliseconds / F.lit(1000)))
        if field.target_type == "date":
            value = value.cast("date")
    else:
        value = value.cast(field.target_type)

    if field.normalize == "trim":
        value = F.trim(value)
    elif field.normalize == "lower":
        value = F.lower(F.trim(value))
    elif field.normalize == "upper":
        value = F.upper(F.trim(value))
    elif field.normalize == "digits":
        value = F.regexp_replace(F.trim(value), r"\D", "")
    return value.alias(field.name)


def _quality_condition(rule: EntityRule):
    from pyspark.sql import functions as F

    condition = F.lit(True)
    for field in rule.fields:
        column = F.col(field.name)
        if field.required:
            condition = condition & column.isNotNull()
            if field.target_type == "string":
                condition = condition & (F.length(column) > 0)
        if field.allowed_values:
            allowed = column.isin(*field.allowed_values)
            condition = condition & (
                allowed if field.required else (column.isNull() | allowed)
            )
        if field.pattern:
            pattern_match = column.rlike(field.pattern)
            condition = condition & (
                pattern_match if field.required else (column.isNull() | pattern_match)
            )
        if field.minimum is not None:
            minimum = column >= F.lit(field.minimum)
            condition = condition & (
                minimum if field.required else (column.isNull() | minimum)
            )
        if field.maximum is not None:
            maximum = column <= F.lit(field.maximum)
            condition = condition & (
                maximum if field.required else (column.isNull() | maximum)
            )
    return condition


def _deduplicate_and_validate(
    bronze,
    *,
    rule: EntityRule,
    run_id: str,
):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    document = F.from_json(F.col("raw_document"), _source_schema(rule))
    primary_key_field = next(
        field for field in rule.fields if field.name == rule.primary_key
    )
    primary_key_value = _field_expression(document, primary_key_field)
    selected = bronze.select(
        *[_field_expression(document, field) for field in rule.fields],
        F.col("_bronze_source_file").alias("_silver_source_file"),
        F.col("_bronze_ingested_at").alias("_silver_source_ingested_at"),
        F.when(
            primary_key_value.isNull(),
            F.concat(
                F.lit("__invalid__"),
                F.sha2(F.col("raw_document"), 256),
            ),
        )
        .otherwise(primary_key_value.cast("string"))
        .alias("_silver_dedup_key"),
    )
    window = Window.partitionBy("_silver_dedup_key").orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("_silver_source_ingested_at").desc_nulls_last(),
        F.col("_silver_source_file").desc_nulls_last(),
    )
    deduplicated = (
        selected.withColumn("_silver_row_number", F.row_number().over(window))
        .filter(F.col("_silver_row_number") == 1)
        .drop("_silver_row_number", "_silver_dedup_key")
    )
    business_columns = [field.name for field in rule.fields]
    enriched = (
        deduplicated.withColumn(
            "_silver_record_hash",
            F.sha2(F.to_json(F.struct(*business_columns)), 256),
        )
        .withColumn("_silver_airflow_run_id", F.lit(run_id))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    return enriched, _quality_condition(rule)


def _validate_foreign_keys(
    spark,
    data,
    *,
    bucket: str,
    database: str,
    rule: EntityRule,
):
    from pyspark.sql import functions as F

    original_columns = data.columns
    valid = data
    for foreign_key in rule.foreign_keys:
        target_uri = silver_table_uri(
            bucket,
            database,
            foreign_key.target_table,
        )
        parent = (
            spark.read.format("delta")
            .load(target_uri)
            .select(F.col(foreign_key.target_field).alias(foreign_key.field))
            .distinct()
        )
        if foreign_key.required:
            valid = valid.join(
                F.broadcast(parent),
                on=foreign_key.field,
                how="left_semi",
            )
        else:
            null_values = valid.filter(F.col(foreign_key.field).isNull())
            matching_values = valid.filter(F.col(foreign_key.field).isNotNull()).join(
                F.broadcast(parent),
                on=foreign_key.field,
                how="left_semi",
            )
            valid = null_values.unionByName(matching_values)
    return valid.select(*original_columns)


def _merge_silver(
    spark,
    data,
    *,
    output_uri: str,
    primary_key: str,
) -> dict[str, int]:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    valid_count = data.count()
    if not DeltaTable.isDeltaTable(spark, output_uri):
        if valid_count:
            data.write.format("delta").mode("append").save(output_uri)
        return {
            "inserted": valid_count,
            "updated": 0,
            "unchanged": 0,
            "rows_written": valid_count,
        }

    target = (
        spark.read.format("delta")
        .load(output_uri)
        .select(
            F.col(primary_key).alias("_target_primary_key"),
            F.col("updated_at").alias("_target_updated_at"),
            F.col("_silver_record_hash").alias("_target_record_hash"),
        )
    )
    comparison = data.join(
        target,
        data[primary_key] == target["_target_primary_key"],
        "left",
    )
    inserted = comparison.filter(F.col("_target_primary_key").isNull()).count()
    update_condition = F.col("_target_primary_key").isNotNull() & (
        (F.col("updated_at") > F.col("_target_updated_at"))
        | (
            (F.col("updated_at") == F.col("_target_updated_at"))
            & (F.col("_silver_record_hash") != F.col("_target_record_hash"))
        )
    )
    updated = comparison.filter(update_condition).count()

    if inserted or updated:
        (
            DeltaTable.forPath(spark, output_uri)
            .alias("target")
            .merge(
                data.alias("source"),
                f"target.{primary_key} = source.{primary_key}",
            )
            .whenMatchedUpdateAll(
                condition=(
                    "source.updated_at > target.updated_at OR "
                    "(source.updated_at = target.updated_at AND "
                    "source._silver_record_hash <> "
                    "target._silver_record_hash)"
                )
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

    return {
        "inserted": inserted,
        "updated": updated,
        "unchanged": valid_count - inserted - updated,
        "rows_written": inserted + updated,
    }


def process_table(
    spark,
    *,
    bucket: str,
    database: str,
    table: str,
    run_id: str,
) -> dict[str, Any]:
    from pyspark.storagelevel import StorageLevel

    rule = ENTITY_RULES[table]
    input_uri = bronze_table_uri(bucket, database, table)
    output_uri = silver_table_uri(bucket, database, table)
    bronze = spark.read.format("delta").load(input_uri)
    records_read = bronze.count()

    candidates, quality_condition = _deduplicate_and_validate(
        bronze,
        rule=rule,
        run_id=run_id,
    )
    candidates.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        deduplicated_count = candidates.count()
        field_valid = candidates.filter(quality_condition)
        field_valid_count = field_valid.count()
        relational_valid = _validate_foreign_keys(
            spark,
            field_valid,
            bucket=bucket,
            database=database,
            rule=rule,
        )
        relational_valid.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            records_valid = relational_valid.count()
            merge_metrics = _merge_silver(
                spark,
                relational_valid,
                output_uri=output_uri,
                primary_key=rule.primary_key,
            )
        finally:
            relational_valid.unpersist()
    finally:
        candidates.unpersist()

    result = {
        "table": table,
        "primary_key": rule.primary_key,
        "input_uri": input_uri,
        "output_uri": output_uri,
        "records_read": records_read,
        "records_deduplicated": deduplicated_count,
        "duplicates_removed": records_read - deduplicated_count,
        "field_rejected": deduplicated_count - field_valid_count,
        "referential_rejected": field_valid_count - records_valid,
        "records_rejected": deduplicated_count - records_valid,
        "records_valid": records_valid,
        **merge_metrics,
        "status": ("written" if merge_metrics["rows_written"] else "already_current"),
    }
    LOGGER.info("Table processed: %s", result)
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
    tables = parse_pipeline_tables(args.tables)
    spark = create_spark_session(args.endpoint_url)

    try:
        results = [
            process_table(
                spark,
                bucket=args.bucket,
                database=args.database,
                table=table,
                run_id=args.run_id,
            )
            for table in tables
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
            "records_rejected": sum(item["records_rejected"] for item in results),
            "results": results,
        }
        LOGGER.info(
            "Bronze to Silver completed: rows=%s rejected=%s manifest=s3://%s/%s",
            summary["rows_written"],
            summary["records_rejected"],
            args.bucket,
            manifest_key,
        )
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
