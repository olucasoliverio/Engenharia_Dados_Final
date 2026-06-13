"""Airflow DAG for incremental ingestion from MongoDB Atlas to Landing."""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mongo.hooks.mongo import MongoHook

try:
    from airflow.sdk import Variable, dag, get_current_context, task
except ImportError:  # Airflow 2 compatibility
    from airflow.decorators import dag, task
    from airflow.models import Variable
    from airflow.operators.python import get_current_context

from lib.mongodb_landing import (
    build_data_key,
    build_incremental_filter,
    build_manifest,
    build_manifest_key,
    checkpoint_variable_name,
    format_checkpoint,
    parse_checkpoint,
    parse_collections,
    write_json_lines,
)


LOGGER = logging.getLogger(__name__)

DAG_ID = "mongodb_to_landing"
MONGO_CONN_ID = os.getenv("MONGO_CONN_ID", "mongodb_atlas")
S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "ecommerce")
LANDING_BUCKET = os.getenv("LANDING_BUCKET", "datalake")
INCREMENTAL_FIELD = os.getenv("MONGO_INCREMENTAL_FIELD", "updated_at")
EXPECTED_COLLECTIONS = parse_collections(os.getenv("MONGO_COLLECTIONS"))
BATCH_SIZE = int(os.getenv("MONGO_BATCH_SIZE", "1000"))
OVERLAP_HOURS = int(os.getenv("MONGO_CHECKPOINT_OVERLAP_HOURS", "24"))
SCHEDULE = os.getenv("MONGO_TO_LANDING_SCHEDULE", "*/15 * * * *")


@dag(
    dag_id=DAG_ID,
    description="Extrai o MongoDB Atlas para a camada Landing em JSON bruto.",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "engenharia_dados",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["mongodb", "landing", "incremental"],
)
def mongodb_to_landing():
    @task
    def validate_landing_bucket() -> str:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        if not s3_hook.check_for_bucket(LANDING_BUCKET):
            raise AirflowException(
                f"Landing bucket {LANDING_BUCKET!r} does not exist. "
                "Create the Data Lake structure before running this DAG."
            )
        return LANDING_BUCKET

    @task
    def validate_source_collections() -> list[str]:
        mongo_hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
        try:
            client = mongo_hook.get_conn()
            available = set(client[MONGO_DATABASE].list_collection_names())
        finally:
            mongo_hook.close()

        missing = sorted(set(EXPECTED_COLLECTIONS) - available)
        if missing:
            raise AirflowException(
                "MongoDB source is missing required collections: " + ", ".join(missing)
            )
        return list(EXPECTED_COLLECTIONS)

    @task
    def extract_collection(collection_name: str) -> dict:
        context = get_current_context()
        logical_date = context["logical_date"]
        run_id = context["run_id"]
        variable_key = checkpoint_variable_name(
            MONGO_DATABASE,
            collection_name,
        )
        try:
            checkpoint_value = Variable.get(variable_key, default=None)
        except TypeError:  # Airflow 2 uses default_var
            checkpoint_value = Variable.get(variable_key, default_var=None)
        checkpoint = parse_checkpoint(checkpoint_value)
        query = build_incremental_filter(
            checkpoint,
            INCREMENTAL_FIELD,
            OVERLAP_HOURS,
        )
        object_key = build_data_key(
            MONGO_DATABASE,
            collection_name,
            logical_date,
            run_id,
        )

        mongo_hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
        cursor = None
        temp_path = None
        try:
            collection = mongo_hook.get_collection(
                mongo_collection=collection_name,
                mongo_db=MONGO_DATABASE,
            )
            cursor = (
                collection.find(query).sort(INCREMENTAL_FIELD, 1).batch_size(BATCH_SIZE)
            )

            with tempfile.NamedTemporaryFile(
                mode="w",
                suffix=".json",
                delete=False,
            ) as temp_file:
                temp_path = Path(temp_file.name)

            summary = write_json_lines(
                cursor,
                temp_path,
                INCREMENTAL_FIELD,
            )

            uploaded_key = None
            if summary.document_count:
                S3Hook(aws_conn_id=S3_CONN_ID).load_file(
                    filename=str(temp_path),
                    key=object_key,
                    bucket_name=LANDING_BUCKET,
                    replace=True,
                )
                uploaded_key = object_key

            result = {
                "collection": collection_name,
                "document_count": summary.document_count,
                "object_key": uploaded_key,
                "checkpoint_variable": variable_key,
                "checkpoint_before": (
                    format_checkpoint(checkpoint) if checkpoint else None
                ),
                "checkpoint_after": (
                    format_checkpoint(summary.max_updated_at)
                    if summary.max_updated_at
                    else (format_checkpoint(checkpoint) if checkpoint else None)
                ),
            }
            LOGGER.info("Collection extraction completed: %s", result)
            return result
        finally:
            if cursor is not None:
                cursor.close()
            mongo_hook.close()
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)

    @task
    def write_run_manifest(results: list[dict]) -> str:
        context = get_current_context()
        logical_date = context["logical_date"]
        run_id = context["run_id"]
        manifest_key = build_manifest_key(logical_date, run_id)
        manifest = build_manifest(
            dag_id=DAG_ID,
            run_id=run_id,
            logical_date=logical_date,
            database=MONGO_DATABASE,
            results=results,
        )
        S3Hook(aws_conn_id=S3_CONN_ID).load_string(
            string_data=manifest,
            key=manifest_key,
            bucket_name=LANDING_BUCKET,
            replace=True,
        )
        for result in results:
            checkpoint_after = result["checkpoint_after"]
            if checkpoint_after is not None:
                Variable.set(
                    result["checkpoint_variable"],
                    checkpoint_after,
                )
        LOGGER.info("Run manifest written to s3://%s/%s", LANDING_BUCKET, manifest_key)
        return manifest_key

    bucket = validate_landing_bucket()
    collections = validate_source_collections()
    extractions = extract_collection.expand(collection_name=collections)
    bucket >> extractions
    write_run_manifest(extractions)


mongodb_to_landing_dag = mongodb_to_landing()
