"""Convert MongoDB Extended JSON Lines from Landing into Bronze Delta tables."""

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

from dags.lib.landing_bronze import (  # noqa: E402
    bronze_table_uri,
    build_manifest,
    build_manifest_key,
    build_spark_conf,
    landing_collection_uri,
    parse_logical_date,
    parse_pipeline_collections,
)


LOGGER = logging.getLogger(__name__)
SOURCE_FILE_COLUMN = "_bronze_source_file"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Converte arquivos JSON da Landing em tabelas Bronze Delta."
    )
    parser.add_argument("--bucket", default="datalake")
    parser.add_argument("--database", default="ecommerce")
    parser.add_argument("--collections", default=None)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--logical-date", required=True)
    parser.add_argument("--endpoint-url", required=True)
    return parser.parse_args()


def create_spark_session(endpoint_url: str):
    """Create a Delta-enabled Spark session configured for S3A."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("landing-to-bronze")
    for key, value in build_spark_conf(endpoint_url).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def process_collection(
    spark,
    *,
    bucket: str,
    database: str,
    collection: str,
    run_id: str,
    logical_date: datetime,
) -> dict[str, Any]:
    """Append unprocessed Landing files to one Bronze Delta table."""
    from delta.tables import DeltaTable
    from pyspark.errors import AnalysisException
    from pyspark.sql import functions as F
    from pyspark.storagelevel import StorageLevel

    input_uri = landing_collection_uri(bucket, database, collection)
    output_uri = bronze_table_uri(bucket, database, collection)

    try:
        source = (
            spark.read.option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.json")
            .text(input_uri)
            .withColumnRenamed("value", "raw_document")
        )
    except AnalysisException as error:
        if "PATH_NOT_FOUND" not in str(error):
            raise
        return _empty_result(collection, input_uri, output_uri)

    enriched = (
        source.withColumn(SOURCE_FILE_COLUMN, F.input_file_name())
        .withColumn(
            "_bronze_extraction_date",
            F.regexp_extract(
                F.col(SOURCE_FILE_COLUMN),
                r"/extraction_date=([^/]+)/",
                1,
            ).cast("date"),
        )
        .withColumn(
            "_bronze_landing_run_id",
            F.regexp_extract(
                F.col(SOURCE_FILE_COLUMN),
                r"/run_id=([^/]+)/",
                1,
            ),
        )
        .withColumn("_bronze_airflow_run_id", F.lit(run_id))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
        .withColumn("ingestion_date", F.lit(logical_date.date()).cast("date"))
    )

    if DeltaTable.isDeltaTable(spark, output_uri):
        processed_files = (
            spark.read.format("delta")
            .load(output_uri)
            .select(SOURCE_FILE_COLUMN)
            .distinct()
        )
        pending = enriched.join(
            processed_files,
            on=SOURCE_FILE_COLUMN,
            how="left_anti",
        )
    else:
        pending = enriched

    pending.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        rows_written = pending.count()
        source_files = pending.select(SOURCE_FILE_COLUMN).distinct().count()
        if rows_written:
            (
                pending.write.format("delta")
                .mode("append")
                .option("mergeSchema", "true")
                .partitionBy("ingestion_date")
                .save(output_uri)
            )
    finally:
        pending.unpersist()

    result = {
        "collection": collection,
        "input_uri": input_uri,
        "output_uri": output_uri,
        "source_files": source_files,
        "rows_written": rows_written,
        "status": "written" if rows_written else "already_processed",
    }
    LOGGER.info("Collection processed: %s", result)
    return result


def write_manifest(
    *,
    spark,
    bucket: str,
    run_id: str,
    logical_date: datetime,
    database: str,
    results: list[dict[str, Any]],
) -> str:
    """Write one JSON audit manifest after all Delta writes succeed."""
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
    collections = parse_pipeline_collections(args.collections)
    spark = create_spark_session(args.endpoint_url)

    try:
        results = [
            process_collection(
                spark,
                bucket=args.bucket,
                database=args.database,
                collection=collection,
                run_id=args.run_id,
                logical_date=logical_date,
            )
            for collection in collections
        ]
        manifest_key = write_manifest(
            spark=spark,
            bucket=args.bucket,
            run_id=args.run_id,
            logical_date=logical_date,
            database=args.database,
            results=results,
        )
        LOGGER.info(
            "Landing to Bronze completed: rows=%s files=%s manifest=s3://%s/%s",
            sum(item["rows_written"] for item in results),
            sum(item["source_files"] for item in results),
            args.bucket,
            manifest_key,
        )
        print(
            json.dumps(
                {
                    "manifest_key": manifest_key,
                    "results": results,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        return 0
    finally:
        spark.stop()


def _empty_result(
    collection: str,
    input_uri: str,
    output_uri: str,
) -> dict[str, Any]:
    return {
        "collection": collection,
        "input_uri": input_uri,
        "output_uri": output_uri,
        "source_files": 0,
        "rows_written": 0,
        "status": "no_landing_files",
    }


if __name__ == "__main__":
    sys.exit(main())
