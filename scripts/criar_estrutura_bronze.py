"""Create and validate the Bronze Delta structure in MinIO or Amazon S3."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from scripts.lib.object_storage import (
        as_utc_iso,
        build_s3_client,
        ensure_bucket,
        is_missing_object_error,
        manifest_matches,
        object_exists,
    )
except ModuleNotFoundError:
    from lib.object_storage import (  # type: ignore[no-redef]
        as_utc_iso,
        build_s3_client,
        ensure_bucket,
        is_missing_object_error,
        manifest_matches,
        object_exists,
    )


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "bronze_structure.json"
STRUCTURE_MANIFEST_NAME = "_structure.json"
PREFIX_MARKER_NAME = "_READY"
VALID_NAME = re.compile(r"^[a-z][a-z0-9_]*$")


@dataclass(frozen=True)
class BronzeConfig:
    bucket: str
    database: str
    layer: str
    tables: tuple[str, ...]
    partition_columns: tuple[str, ...]


def load_config(path: Path) -> BronzeConfig:
    """Load and validate the versioned Bronze contract."""
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
    _validate_names(tables, "table")
    _validate_names(partition_columns, "partition column")

    layer = str(payload["layer"])
    if layer != "bronze":
        raise ValueError("Bronze structure layer must be 'bronze'")

    return BronzeConfig(
        bucket=str(payload["bucket"]),
        database=str(payload["database"]),
        layer=layer,
        tables=tables,
        partition_columns=partition_columns,
    )


def table_prefix(config: BronzeConfig, table: str) -> str:
    """Return the root prefix of one Delta table."""
    return f"{config.layer}/{config.database}/{table}/"


def table_marker_key(config: BronzeConfig, table: str) -> str:
    """Return the hidden object that materializes one table prefix."""
    return f"{table_prefix(config, table)}{PREFIX_MARKER_NAME}"


def delta_log_prefix(config: BronzeConfig, table: str) -> str:
    """Return the transaction log prefix created by the first Delta write."""
    return f"{table_prefix(config, table)}_delta_log/"


def structure_manifest_key(config: BronzeConfig) -> str:
    """Return the Bronze control manifest object key."""
    return f"{config.layer}/_control/{STRUCTURE_MANIFEST_NAME}"


def build_structure_manifest(
    config: BronzeConfig,
    generated_at: datetime,
) -> str:
    """Build an auditable description of the expected Bronze structure."""
    payload = {
        "bucket": config.bucket,
        "database": config.database,
        "layer": config.layer,
        "format": "delta",
        "generated_at": as_utc_iso(generated_at),
        "partition_columns": list(config.partition_columns),
        "source": {
            "layer": "landing",
            "format": "mongodb_extended_json_canonical_lines",
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


def initialize_bronze(
    s3_client: Any,
    config: BronzeConfig,
    region: str = "us-east-1",
    enable_versioning: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Idempotently create the Bronze prefixes and structure manifest."""
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


def validate_bronze(s3_client: Any, config: BronzeConfig) -> list[str]:
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

    if manifest_key not in {
        path.removeprefix(f"s3://{config.bucket}/") for path in invalid
    }:
        expected = build_structure_manifest(config, datetime.now(timezone.utc))
        if not manifest_matches(
            s3_client,
            config.bucket,
            manifest_key,
            expected,
        ):
            invalid.append(f"s3://{config.bucket}/{manifest_key} (divergente)")

    return invalid


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cria e valida a estrutura Bronze Delta no MinIO ou S3."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Contrato JSON da Bronze.",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.getenv("S3_ENDPOINT_URL"),
        help="Endpoint do MinIO. Omita para usar Amazon S3.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        help="Regiao do bucket.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Apenas valida a estrutura sem criar objetos.",
    )
    parser.add_argument(
        "--no-versioning",
        action="store_true",
        help="Nao habilita versionamento no bucket.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        config = load_config(args.config)
        client = build_s3_client(args.endpoint_url, args.region)

        if not args.validate_only:
            result = initialize_bronze(
                client,
                config,
                region=args.region,
                enable_versioning=not args.no_versioning,
            )
            print(
                "Estrutura criada/atualizada: "
                f"{result['table_count']} tabelas em "
                f"s3://{result['bucket']}/{config.layer}/"
            )
            print(
                f"Marcadores criados: {len(result['markers_created'])}; "
                "manifesto atualizado: "
                f"{'sim' if result['manifest_updated'] else 'nao'}"
            )

        invalid = validate_bronze(client, config)
        if invalid:
            print("Estrutura Bronze incompleta ou divergente:")
            for path in invalid:
                print(f"  - {path}")
            return 1

        print(f"Estrutura Bronze valida: s3://{config.bucket}/{config.layer}/")
        return 0
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Erro de configuracao: {error}")
        return 2
    except Exception as error:
        print(f"Erro ao acessar o object storage: {error}")
        return 1


def _validate_names(names: tuple[str, ...], field: str) -> None:
    if not names:
        raise ValueError(f"Bronze structure must contain at least one {field}")
    if len(names) != len(set(names)):
        raise ValueError(f"Bronze structure contains duplicate {field}s")
    if any(
        not isinstance(name, str) or not VALID_NAME.fullmatch(name) for name in names
    ):
        raise ValueError(
            f"Bronze {field} names must use lowercase letters, numbers and underscores"
        )


if __name__ == "__main__":
    sys.exit(main())
