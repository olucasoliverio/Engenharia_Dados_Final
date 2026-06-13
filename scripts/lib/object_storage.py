"""Shared operations for MinIO and Amazon S3 infrastructure scripts."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any


def build_s3_client(endpoint_url: str | None, region: str):
    """Create a boto3 S3 client using the standard AWS credential chain."""
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
    )


def ensure_bucket(s3_client: Any, bucket: str, region: str) -> bool:
    """Create the bucket when absent and return whether it was created."""
    try:
        s3_client.head_bucket(Bucket=bucket)
        return False
    except Exception as error:
        if not is_missing_bucket_error(error):
            raise

    create_args: dict[str, Any] = {"Bucket": bucket}
    if region != "us-east-1":
        create_args["CreateBucketConfiguration"] = {
            "LocationConstraint": region,
        }
    s3_client.create_bucket(**create_args)
    return True


def object_exists(s3_client: Any, bucket: str, key: str) -> bool:
    """Return whether an object exists, preserving unexpected S3 errors."""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except Exception as error:
        if is_missing_object_error(error):
            return False
        raise


def manifest_matches(
    s3_client: Any,
    bucket: str,
    key: str,
    expected_manifest: str,
) -> bool:
    """Compare a structure contract while ignoring its generation time."""
    if not object_exists(s3_client, bucket, key):
        return False

    current_body = s3_client.get_object(Bucket=bucket, Key=key)["Body"].read()
    current = json.loads(current_body)
    expected = json.loads(expected_manifest)
    current.pop("generated_at", None)
    expected.pop("generated_at", None)
    return current == expected


def error_code(error: Exception) -> str | None:
    """Return the S3-compatible error code when available."""
    response = getattr(error, "response", {})
    return response.get("Error", {}).get("Code")


def is_missing_bucket_error(error: Exception) -> bool:
    """Return whether an error represents a missing bucket."""
    return error_code(error) in {"404", "NoSuchBucket", "NotFound"}


def is_missing_object_error(error: Exception) -> bool:
    """Return whether an error represents a missing object."""
    return error_code(error) in {"404", "NoSuchKey", "NotFound"}


def as_utc_iso(value: datetime) -> str:
    """Format a datetime as an ISO-8601 UTC value."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
