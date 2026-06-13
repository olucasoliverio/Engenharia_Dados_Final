"""Reusable structure operations for Delta Lake medallion layers."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .object_storage import (
    as_utc_iso,
    ensure_bucket,
    is_missing_object_error,
    manifest_matches,
    object_exists,
)


STRUCTURE_MANIFEST_NAME = "_structure.json"
PREFIX_MARKER_NAME = "_READY"
VALID_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class DeltaLayerConfig:
    bucket: str
    database: str
    layer: str
    tables: tuple[str, ...]
    partition_columns: tuple[str, ...]
    source_layer: str
    source_format: str


def load_delta_config(
    path: Path,
    *,
    expected_layer: str,
    source_layer: str,
    source_format: str,
) -> DeltaLayerConfig:
    """Load and validate a versioned Delta layer contract."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    required_fields = {
        "bucket",
        "database",
        "layer",
        "tables",
        "partition_columns",
    }
    missing = sorted(required_fields - payload.keys())
    if missing:
        raise ValueError(f"Missing configuration fields: {', '.join(missing)}")

    tables = tuple(payload["tables"])
    partition_columns = tuple(payload["partition_columns"])
    _validate_names(tables, "table", required=True)
    _validate_names(partition_columns, "partition column", required=False)

    layer = str(payload["layer"])
    if layer != expected_layer:
        raise ValueError(
            f"{expected_layer.title()} structure layer must be '{expected_layer}'"
        )

    return DeltaLayerConfig(
        bucket=str(payload["bucket"]),
        database=str(payload["database"]),
        layer=layer,
        tables=tables,
        partition_columns=partition_columns,
        source_layer=source_layer,
        source_format=source_format,
    )


def table_prefix(config: DeltaLayerConfig, table: str) -> str:
    """Return the root prefix of one Delta table."""
    return f"{config.layer}/{config.database}/{table}/"


def table_marker_key(config: DeltaLayerConfig, table: str) -> str:
    """Return the hidden object that materializes one table prefix."""
    return f"{table_prefix(config, table)}{PREFIX_MARKER_NAME}"


def delta_log_prefix(config: DeltaLayerConfig, table: str) -> str:
    """Return the transaction log prefix created by the first Delta write."""
    return f"{table_prefix(config, table)}_delta_log/"


def structure_manifest_key(config: DeltaLayerConfig) -> str:
    """Return the layer control manifest object key."""
    return f"{config.layer}/_control/{STRUCTURE_MANIFEST_NAME}"


def build_structure_manifest(
    config: DeltaLayerConfig,
    generated_at: datetime,
) -> str:
    """Build an auditable description of the expected Delta structure."""
    payload = {
        "bucket": config.bucket,
        "database": config.database,
        "layer": config.layer,
        "format": "delta",
        "generated_at": as_utc_iso(generated_at),
        "partition_columns": list(config.partition_columns),
        "source": {
            "layer": config.source_layer,
            "format": config.source_format,
        },
        "tables": [
            {
                "name": table,
                "prefix": table_prefix(config, table),
                "delta_log_prefix": delta_log_prefix(config, table),
            }
            for table in config.tables
        ],
        "control_prefix": f"{config.layer}/_control/",
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def initialize_delta_layer(
    s3_client: Any,
    config: DeltaLayerConfig,
    region: str = "us-east-1",
    enable_versioning: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Idempotently create Delta table prefixes and a structure manifest."""
    generated_at = generated_at or datetime.now(timezone.utc)
    bucket_created = ensure_bucket(s3_client, config.bucket, region)

    if enable_versioning:
        s3_client.put_bucket_versioning(
            Bucket=config.bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

    marker_keys = []
    markers_created = []
    for table in config.tables:
        key = table_marker_key(config, table)
        marker_keys.append(key)
        if not object_exists(s3_client, config.bucket, key):
            s3_client.put_object(
                Bucket=config.bucket,
                Key=key,
                Body=b"",
                ContentType="application/octet-stream",
                Metadata={
                    "layer": config.layer,
                    "database": config.database,
                    "table": table,
                    "format": "delta",
                },
            )
            markers_created.append(key)

    manifest_key = structure_manifest_key(config)
    manifest = build_structure_manifest(config, generated_at)
    manifest_updated = not manifest_matches(
        s3_client,
        config.bucket,
        manifest_key,
        manifest,
    )
    if manifest_updated:
        s3_client.put_object(
            Bucket=config.bucket,
            Key=manifest_key,
            Body=manifest.encode("utf-8"),
            ContentType="application/json",
        )

    return {
        "bucket": config.bucket,
        "bucket_created": bucket_created,
        "versioning_enabled": enable_versioning,
        "table_count": len(config.tables),
        "marker_keys": marker_keys,
        "markers_created": markers_created,
        "manifest_key": manifest_key,
        "manifest_updated": manifest_updated,
    }


def validate_delta_layer(
    s3_client: Any,
    config: DeltaLayerConfig,
) -> list[str]:
    """Return invalid required objects; an empty list means valid structure."""
    invalid = []
    manifest_key = structure_manifest_key(config)
    required_keys = [
        *(table_marker_key(config, name) for name in config.tables),
        manifest_key,
    ]

    try:
        s3_client.head_bucket(Bucket=config.bucket)
    except Exception:
        return [f"s3://{config.bucket}"]

    for key in required_keys:
        try:
            s3_client.head_object(Bucket=config.bucket, Key=key)
        except Exception as error:
            if is_missing_object_error(error):
                invalid.append(f"s3://{config.bucket}/{key}")
            else:
                raise

    missing_keys = {path.removeprefix(f"s3://{config.bucket}/") for path in invalid}
    if manifest_key not in missing_keys:
        expected = build_structure_manifest(config, datetime.now(timezone.utc))
        if not manifest_matches(
            s3_client,
            config.bucket,
            manifest_key,
            expected,
        ):
            invalid.append(f"s3://{config.bucket}/{manifest_key} (divergente)")

    return invalid


def _validate_names(
    names: tuple[str, ...],
    field: str,
    *,
    required: bool,
) -> None:
    if required and not names:
        raise ValueError(f"Delta layer structure must contain at least one {field}")
    if len(names) != len(set(names)):
        raise ValueError(f"Delta layer structure contains duplicate {field}s")
    if any(
        not isinstance(name, str) or not VALID_NAME.fullmatch(name) for name in names
    ):
        raise ValueError(
            f"Delta {field} names must use lowercase letters, numbers and underscores"
        )
