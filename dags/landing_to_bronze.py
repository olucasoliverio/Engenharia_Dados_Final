"""Airflow DAG for converting Landing JSON files into Bronze Delta tables."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)

try:
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2 compatibility
    from airflow.decorators import dag, task

from lib.landing_bronze import (
    build_spark_conf,
    parse_pipeline_collections,
    spark_packages,
)


DAG_ID = "landing_to_bronze"
S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark_default")
BUCKET = os.getenv("LANDING_BUCKET", "datalake")
DATABASE = os.getenv("MONGO_DATABASE", "ecommerce")
COLLECTIONS = parse_pipeline_collections(os.getenv("MONGO_COLLECTIONS"))
SCHEDULE = os.getenv("LANDING_TO_BRONZE_SCHEDULE", "5-59/15 * * * *")
SPARK_APPLICATION = os.getenv(
    "LANDING_TO_BRONZE_APPLICATION",
    "/opt/airflow/spark_jobs/landing_to_bronze.py",
)
SPARK_S3_ENDPOINT = os.getenv("SPARK_S3_ENDPOINT", "http://minio:9000")
SPARK_PACKAGES = os.getenv("SPARK_PACKAGES", spark_packages())


def _spark_env() -> dict[str, str]:
    keys = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_DEFAULT_REGION",
    )
    return {key: os.environ[key] for key in keys if os.getenv(key)}


@dag(
    dag_id=DAG_ID,
    description="Converte JSONs da Landing em tabelas Bronze Delta.",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "engenharia_dados",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["landing", "bronze", "spark", "delta"],
)
def landing_to_bronze():
    @task
    def validate_data_lake() -> dict[str, int]:
        client = S3Hook(aws_conn_id=S3_CONN_ID).get_conn()
        try:
            client.head_bucket(Bucket=BUCKET)
        except Exception as error:
            raise AirflowException(
                f"Data Lake bucket {BUCKET!r} is not accessible."
            ) from error

        landing_files = 0
        for collection in COLLECTIONS:
            marker_key = f"bronze/{DATABASE}/{collection}/_READY"
            try:
                client.head_object(Bucket=BUCKET, Key=marker_key)
            except Exception as error:
                raise AirflowException(
                    f"Bronze structure is missing s3://{BUCKET}/{marker_key}."
                ) from error

            response = client.list_objects_v2(
                Bucket=BUCKET,
                Prefix=f"landing/{DATABASE}/{collection}/",
            )
            collection_files = sum(
                1
                for item in response.get("Contents", [])
                if item["Key"].endswith(".json")
            )
            if not collection_files:
                raise AirflowException(
                    f"Landing has no JSON files for collection {collection!r}."
                )
            landing_files += collection_files

        return {
            "collections": len(COLLECTIONS),
            "landing_files": landing_files,
        }

    validate = validate_data_lake()
    submit = SparkSubmitOperator(
        task_id="convert_landing_to_bronze",
        conn_id=SPARK_CONN_ID,
        application=SPARK_APPLICATION,
        name="landing-to-bronze",
        packages=SPARK_PACKAGES,
        conf=build_spark_conf(SPARK_S3_ENDPOINT),
        env_vars=_spark_env(),
        application_args=[
            "--bucket",
            BUCKET,
            "--database",
            DATABASE,
            "--collections",
            ",".join(COLLECTIONS),
            "--run-id",
            "{{ run_id }}",
            "--logical-date",
            "{{ dag_run.logical_date or dag_run.run_after }}",
            "--endpoint-url",
            SPARK_S3_ENDPOINT,
        ],
        verbose=True,
        execution_timeout=timedelta(hours=2),
    )
    validate >> submit


landing_to_bronze_dag = landing_to_bronze()
