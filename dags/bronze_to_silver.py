"""Airflow DAG for cleaning Bronze Delta tables into the Silver layer."""

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

from lib.bronze_silver import (
    build_spark_conf,
    parse_pipeline_tables,
    spark_packages,
)


DAG_ID = "bronze_to_silver"
S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark_default")
BUCKET = os.getenv(
    "DATA_LAKE_BUCKET",
    os.getenv("LANDING_BUCKET", "datalake"),
)
DATABASE = os.getenv("MONGO_DATABASE", "ecommerce")
TABLES = parse_pipeline_tables(os.getenv("SILVER_TABLES"))
SCHEDULE = os.getenv("BRONZE_TO_SILVER_SCHEDULE", "10-59/15 * * * *")
SPARK_APPLICATION = os.getenv(
    "BRONZE_TO_SILVER_APPLICATION",
    "/opt/airflow/spark_jobs/bronze_to_silver.py",
)
SPARK_S3_ENDPOINT = os.getenv("SPARK_S3_ENDPOINT", "http://minio:9000")
SPARK_PACKAGES = os.getenv("SPARK_PACKAGES", spark_packages())


def _spark_env() -> dict[str, str]:
    keys = (
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "AWS_DEFAULT_REGION",
        "SPARK_LOG_LEVEL",
    )
    return {key: os.environ[key] for key in keys if os.getenv(key)}


@dag(
    dag_id=DAG_ID,
    description="Limpa, padroniza e deduplica tabelas Bronze na Silver.",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "engenharia_dados",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["bronze", "silver", "spark", "delta", "quality"],
)
def bronze_to_silver():
    @task
    def validate_data_lake() -> dict[str, int]:
        client = S3Hook(aws_conn_id=S3_CONN_ID).get_conn()
        try:
            client.head_bucket(Bucket=BUCKET)
        except Exception as error:
            raise AirflowException(
                f"Data Lake bucket {BUCKET!r} is not accessible."
            ) from error

        for table in TABLES:
            silver_marker = f"silver/{DATABASE}/{table}/_READY"
            try:
                client.head_object(Bucket=BUCKET, Key=silver_marker)
            except Exception as error:
                raise AirflowException(
                    f"Silver structure is missing s3://{BUCKET}/{silver_marker}."
                ) from error

            delta_log = client.list_objects_v2(
                Bucket=BUCKET,
                Prefix=f"bronze/{DATABASE}/{table}/_delta_log/",
                MaxKeys=1,
            )
            if not delta_log.get("KeyCount"):
                raise AirflowException(f"Bronze Delta table is missing for {table!r}.")

        return {"tables": len(TABLES)}

    validate = validate_data_lake()
    submit = SparkSubmitOperator(
        task_id="clean_bronze_to_silver",
        conn_id=SPARK_CONN_ID,
        application=SPARK_APPLICATION,
        name="bronze-to-silver",
        packages=SPARK_PACKAGES,
        conf=build_spark_conf(SPARK_S3_ENDPOINT),
        env_vars=_spark_env(),
        application_args=[
            "--bucket",
            BUCKET,
            "--database",
            DATABASE,
            "--tables",
            ",".join(TABLES),
            "--run-id",
            "{{ run_id }}",
            "--logical-date",
            "{{ logical_date }}",
            "--endpoint-url",
            SPARK_S3_ENDPOINT,
        ],
        verbose=True,
        execution_timeout=timedelta(hours=2),
    )
    validate >> submit


bronze_to_silver_dag = bronze_to_silver()
