"""Airflow DAG for building the Gold dimensional model from Silver."""

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

from lib.silver_gold import (
    GOLD_MODELS,
    SILVER_TABLES,
    build_spark_conf,
    parse_gold_models,
    spark_packages,
)


DAG_ID = "silver_to_gold"
S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
SPARK_CONN_ID = os.getenv("SPARK_CONN_ID", "spark_default")
BUCKET = os.getenv(
    "DATA_LAKE_BUCKET",
    os.getenv("LANDING_BUCKET", "datalake"),
)
DATABASE = os.getenv("MONGO_DATABASE", "ecommerce")
MODELS = parse_gold_models(os.getenv("GOLD_TABLES"))
SCHEDULE = os.getenv("SILVER_TO_GOLD_SCHEDULE", "15-59/15 * * * *")
SPARK_APPLICATION = os.getenv(
    "SILVER_TO_GOLD_APPLICATION",
    "/opt/airflow/spark_jobs/silver_to_gold.py",
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
    description="Gera dimensões, fatos e métricas Gold a partir da Silver.",
    schedule=SCHEDULE,
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "engenharia_dados",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["silver", "gold", "spark", "delta", "bi"],
)
def silver_to_gold():
    @task
    def validate_data_lake() -> dict[str, int]:
        client = S3Hook(aws_conn_id=S3_CONN_ID).get_conn()
        try:
            client.head_bucket(Bucket=BUCKET)
        except Exception as error:
            raise AirflowException(
                f"Data Lake bucket {BUCKET!r} is not accessible."
            ) from error

        required_silver = {
            source for model in MODELS for source in GOLD_MODELS[model].source_tables
        }
        for table in SILVER_TABLES:
            if table not in required_silver:
                continue
            delta_log = client.list_objects_v2(
                Bucket=BUCKET,
                Prefix=f"silver/{DATABASE}/{table}/_delta_log/",
                MaxKeys=1,
            )
            if not delta_log.get("KeyCount"):
                raise AirflowException(f"Silver Delta table is missing for {table!r}.")

        for model in MODELS:
            marker = f"gold/{DATABASE}/{model}/_READY"
            try:
                client.head_object(Bucket=BUCKET, Key=marker)
            except Exception as error:
                raise AirflowException(
                    f"Gold structure is missing s3://{BUCKET}/{marker}."
                ) from error

        return {
            "silver_tables": len(required_silver),
            "gold_models": len(MODELS),
        }

    validate = validate_data_lake()
    submit = SparkSubmitOperator(
        task_id="build_silver_to_gold",
        conn_id=SPARK_CONN_ID,
        application=SPARK_APPLICATION,
        name="silver-to-gold",
        packages=SPARK_PACKAGES,
        conf=build_spark_conf(SPARK_S3_ENDPOINT),
        env_vars=_spark_env(),
        application_args=[
            "--bucket",
            BUCKET,
            "--database",
            DATABASE,
            "--models",
            ",".join(MODELS),
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


silver_to_gold_dag = silver_to_gold()
