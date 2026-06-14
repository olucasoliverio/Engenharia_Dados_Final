"""Pure helpers for the Landing to Bronze Spark pipeline."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

try:
    from .mongodb_landing import DEFAULT_COLLECTIONS, parse_collections
except ImportError:  # Airflow may parse helper modules as standalone files
    from lib.mongodb_landing import DEFAULT_COLLECTIONS, parse_collections


DEFAULT_DELTA_VERSION = "3.3.1"
DEFAULT_HADOOP_AWS_VERSION = "3.3.4"


def parse_pipeline_collections(raw_value: str | None) -> tuple[str, ...]:
    """Parse collections using the same contract as MongoDB to Landing."""
    return parse_collections(raw_value)


def landing_collection_uri(
    bucket: str,
    database: str,
    collection: str,
    scheme: str = "s3a",
) -> str:
    """Return the recursive input path for one Landing collection."""
    return (
        f"{scheme}://{_safe_component(bucket)}/landing/"
        f"{_safe_component(database)}/{_safe_component(collection)}/"
    )


def bronze_table_uri(
    bucket: str,
    database: str,
    collection: str,
    scheme: str = "s3a",
) -> str:
    """Return the Delta table path for one Bronze collection."""
    return (
        f"{scheme}://{_safe_component(bucket)}/bronze/"
        f"{_safe_component(database)}/{_safe_component(collection)}/"
    )


def build_manifest_key(logical_date: datetime, run_id: str) -> str:
    """Build the auditable Bronze manifest key for one Airflow run."""
    ingestion_date = _as_utc(logical_date).date().isoformat()
    return (
        "bronze/_control/landing_to_bronze/"
        f"ingestion_date={ingestion_date}/"
        f"run_id={_safe_component(run_id)}/manifest.json"
    )


def build_manifest(
    *,
    run_id: str,
    logical_date: datetime,
    database: str,
    results: Sequence[Mapping[str, Any]],
) -> str:
    """Serialize the processing summary written after all tables succeed."""
    ordered_results = sorted(results, key=lambda item: str(item["collection"]))
    payload = {
        "dag_id": "landing_to_bronze",
        "run_id": run_id,
        "logical_date": _as_utc(logical_date).isoformat().replace("+00:00", "Z"),
        "source": {
            "layer": "landing",
            "format": "mongodb_extended_json_canonical_lines",
        },
        "target": {
            "layer": "bronze",
            "format": "delta",
            "database": database,
        },
        "total_rows_written": sum(
            int(item["rows_written"]) for item in ordered_results
        ),
        "total_source_files": sum(
            int(item["source_files"]) for item in ordered_results
        ),
        "collections": ordered_results,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def spark_packages(
    delta_version: str = DEFAULT_DELTA_VERSION,
    hadoop_aws_version: str = DEFAULT_HADOOP_AWS_VERSION,
) -> str:
    """Return Maven packages required by Delta Lake and S3A."""
    return ",".join(
        (
            f"io.delta:delta-spark_2.12:{delta_version}",
            f"org.apache.hadoop:hadoop-aws:{hadoop_aws_version}",
        )
    )


def build_spark_conf(endpoint_url: str) -> dict[str, str]:
    """Return Spark settings for Delta Lake over MinIO or Amazon S3."""
    endpoint_url = endpoint_url.rstrip("/")
    use_ssl = endpoint_url.lower().startswith("https://")
    return {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": (
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        ),
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.endpoint": endpoint_url,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": str(use_ssl).lower(),
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "com.amazonaws.auth.EnvironmentVariableCredentialsProvider"
        ),
    }


def default_collections() -> tuple[str, ...]:
    """Expose the source contract without duplicating its table list."""
    return DEFAULT_COLLECTIONS


def parse_logical_date(raw_value: str) -> datetime:
    """Parse an Airflow logical date into an aware UTC datetime."""
    value = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    return _as_utc(value)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_component(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    if not normalized:
        raise ValueError("path component cannot be empty")
    return normalized
