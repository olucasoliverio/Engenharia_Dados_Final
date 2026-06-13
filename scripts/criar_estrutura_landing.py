"""Create and validate the Landing structure in MinIO or Amazon S3."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "landing_structure.json"
STRUCTURE_MANIFEST_NAME = "_structure.json"
PREFIX_MARKER_NAME = "_READY"


@dataclass(frozen=True)
class LandingConfig:
    bucket: str
    database: str
    layer: str
    collections: tuple[str, ...]


def load_config(path: Path) -> LandingConfig:
    """Load and validate the versioned Landing contract."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    required_fields = {"bucket", "database", "layer", "collections"}
    missing = sorted(required_fields - payload.keys())
    if missing:
        raise ValueError(f"Missing configuration fields: {', '.join(missing)}")

    collections = tuple(payload["collections"])
    if not collections:
        raise ValueError("Landing structure must contain at least one collection")
    if len(collections) != len(set(collections)):
        raise ValueError("Landing structure contains duplicate collections")
    if any(not isinstance(name, str) or not name.strip() for name in collections):
        raise ValueError("Landing collection names must be non-empty strings")

    return LandingConfig(
        bucket=str(payload["bucket"]),
        database=str(payload["database"]),
        layer=str(payload["layer"]),
        collections=collections,
    )


def collection_prefix(config: LandingConfig, collection: str) -> str:
    """Return the object prefix used by one MongoDB collection."""
    return f"{config.layer}/{config.database}/{collection}/"


def collection_marker_key(config: LandingConfig, collection: str) -> str:
    """Return the marker object that materializes one S3 prefix."""
    return f"{collection_prefix(config, collection)}{PREFIX_MARKER_NAME}"


def structure_manifest_key(config: LandingConfig) -> str:
    """Return the control manifest object key."""
    return f"{config.layer}/_control/{STRUCTURE_MANIFEST_NAME}"


def build_structure_manifest(
    config: LandingConfig,
    generated_at: datetime,
) -> str:
    """Build an auditable description of the expected Landing structure."""
    payload = {
        "bucket": config.bucket,
        "database": config.database,
        "layer": config.layer,
        "format": "mongodb_extended_json_canonical_lines",
        "generated_at": _as_utc_iso(generated_at),
        "collections": [
            {
                "name": collection,
                "prefix": collection_prefix(config, collection),
            }
            for collection in config.collections
        ],
        "control_prefix": f"{config.layer}/_control/",
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def ensure_bucket(s3_client: Any, bucket: str, region: str) -> bool:
    """Create the bucket when absent and return whether it was created."""
    try:
        s3_client.head_bucket(Bucket=bucket)
        return False
    except Exception as error:
        if not _is_missing_bucket_error(error):
            raise

    create_args: dict[str, Any] = {"Bucket": bucket}
    if region != "us-east-1":
        create_args["CreateBucketConfiguration"] = {
            "LocationConstraint": region,
        }
    s3_client.create_bucket(**create_args)
    return True


def initialize_landing(
    s3_client: Any,
    config: LandingConfig,
    region: str = "us-east-1",
    enable_versioning: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Idempotently create the bucket, prefixes and structure manifest."""
    generated_at = generated_at or datetime.now(timezone.utc)
    bucket_created = ensure_bucket(s3_client, config.bucket, region)

    if enable_versioning:
        s3_client.put_bucket_versioning(
            Bucket=config.bucket,
            VersioningConfiguration={"Status": "Enabled"},
        )

    marker_keys = []
    markers_created = []
    for collection in config.collections:
        key = collection_marker_key(config, collection)
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
                    "collection": collection,
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
        "collection_count": len(config.collections),
        "marker_keys": marker_keys,
        "markers_created": markers_created,
        "manifest_key": manifest_key,
        "manifest_updated": manifest_updated,
    }


def validate_landing(s3_client: Any, config: LandingConfig) -> list[str]:
    """Return missing required objects; an empty list means valid structure."""
    missing = []
    required_keys = [
        *(collection_marker_key(config, name) for name in config.collections),
        structure_manifest_key(config),
    ]

    try:
        s3_client.head_bucket(Bucket=config.bucket)
    except Exception:
        return [f"s3://{config.bucket}"]

    for key in required_keys:
        try:
            s3_client.head_object(Bucket=config.bucket, Key=key)
        except Exception as error:
            if _is_missing_object_error(error):
                missing.append(f"s3://{config.bucket}/{key}")
            else:
                raise
    return missing


def object_exists(s3_client: Any, bucket: str, key: str) -> bool:
    """Return whether an object exists, preserving unexpected S3 errors."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as error:
        if _is_missing_object_error(error):
            return False
        raise


def manifest_matches(
    s3_client: Any,
    bucket: str,
    key: str,
    expected_manifest: str,
) -> bool:
    """Compare the structure contract while ignoring its generation time."""
    if not object_exists(s3_client, bucket, key):
        return False

    current_body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    current = json.loads(current_body)
    expected = json.loads(expected_manifest)
    current.pop("generated_at", None)
    expected.pop("generated_at", None)
    return current == expected


def build_s3_client(endpoint_url: str | None, region: str):
    """Create a boto3 S3 client using the standard AWS credential chain."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cria e valida a estrutura Landing no MinIO ou Amazon S3."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Contrato JSON da Landing.",
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
            result = initialize_landing(
                client,
                config,
                region=args.region,
                enable_versioning=not args.no_versioning,
            )
            print(
                "Estrutura criada/atualizada: "
                f"{result['collection_count']} colecoes em "
                f"s3://{result['bucket']}/{config.layer}/"
            )

        missing = validate_landing(client, config)
        if missing:
            print("Estrutura Landing incompleta:")
            for path in missing:
                print(f"  - {path}")
            return 1

        print(f"Estrutura Landing valida: s3://{config.bucket}/{config.layer}/")
        return 0
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Erro de configuracao: {error}")
        return 2
    except Exception as error:
        print(f"Erro ao acessar o object storage: {error}")
        return 1


def _error_code(error: Exception) -> str | None:
    response = getattr(error, "response", {})
    return response.get("Error", {}).get("Code")


def _is_missing_bucket_error(error: Exception) -> bool:
    return _error_code(error) in {"404", "NoSuchBucket", "NotFound"}


def _is_missing_object_error(error: Exception) -> bool:
    return _error_code(error) in {"404", "NoSuchKey", "NotFound"}


def _as_utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


if __name__ == "__main__":
    sys.exit(main())
