"""Pure helpers for the MongoDB to Landing ingestion DAG."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping


DEFAULT_COLLECTIONS = (
    "clientes",
    "categorias",
    "fornecedores",
    "produtos",
    "cupons",
    "pedidos",
    "itens_pedido",
    "pagamentos",
    "entregas",
    "avaliacoes",
)


@dataclass(frozen=True)
class ExportSummary:
    document_count: int
    max_updated_at: datetime | None


def parse_collections(raw_value: str | None) -> tuple[str, ...]:
    """Parse a comma-separated collection list, preserving its order."""
    if not raw_value:
        return DEFAULT_COLLECTIONS

    collections = tuple(name.strip() for name in raw_value.split(",") if name.strip())
    if not collections:
        raise ValueError("MONGO_COLLECTIONS must contain at least one collection")
    if len(collections) != len(set(collections)):
        raise ValueError("MONGO_COLLECTIONS contains duplicate collection names")
    return collections


def parse_checkpoint(raw_value: str | None) -> datetime | None:
    """Convert an Airflow Variable value into an aware UTC datetime."""
    if not raw_value:
        return None

    checkpoint = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    if checkpoint.tzinfo is None:
        checkpoint = checkpoint.replace(tzinfo=timezone.utc)
    return checkpoint.astimezone(timezone.utc)


def format_checkpoint(value: datetime) -> str:
    """Serialize a checkpoint using an unambiguous UTC ISO-8601 value."""
    return _as_utc(value).isoformat().replace("+00:00", "Z")


def build_incremental_filter(
    checkpoint: datetime | None,
    incremental_field: str,
    overlap_hours: int,
) -> dict[str, Any]:
    """Build a MongoDB filter with an overlap window for timestamp ties."""
    if checkpoint is None:
        return {}
    if overlap_hours < 0:
        raise ValueError("overlap_hours cannot be negative")

    lower_bound = _as_utc(checkpoint) - timedelta(hours=overlap_hours)
    return {incremental_field: {"$gte": lower_bound}}


def checkpoint_variable_name(database: str, collection: str) -> str:
    """Return the Airflow Variable key used by one source collection."""
    return "mongodb_landing_checkpoint__{}_{}".format(
        _safe_component(database),
        _safe_component(collection),
    )


def build_data_key(
    database: str,
    collection: str,
    logical_date: datetime,
    run_id: str,
) -> str:
    """Build an immutable Landing object key for one collection and DAG run."""
    extraction_date = _as_utc(logical_date).date().isoformat()
    return (
        f"landing/{_safe_component(database)}/{_safe_component(collection)}/"
        f"extraction_date={extraction_date}/"
        f"run_id={_safe_component(run_id)}/part-00000.json"
    )


def build_manifest_key(logical_date: datetime, run_id: str) -> str:
    """Build the control manifest key for one complete DAG run."""
    extraction_date = _as_utc(logical_date).date().isoformat()
    return (
        "landing/_control/mongodb_to_landing/"
        f"extraction_date={extraction_date}/"
        f"run_id={_safe_component(run_id)}/manifest.json"
    )


def write_json_lines(
    documents: Iterable[Mapping[str, Any]],
    destination: Path,
    incremental_field: str,
    serializer: Callable[[Mapping[str, Any]], str] | None = None,
) -> ExportSummary:
    """Stream MongoDB documents to JSON Lines without loading them in memory."""
    serializer = serializer or _extended_json_dumps
    document_count = 0
    max_updated_at: datetime | None = None

    with destination.open("w", encoding="utf-8", newline="\n") as output:
        for document in documents:
            output.write(serializer(document))
            output.write("\n")
            document_count += 1

            updated_at = document.get(incremental_field)
            if isinstance(updated_at, datetime):
                updated_at = _as_utc(updated_at)
                if max_updated_at is None or updated_at > max_updated_at:
                    max_updated_at = updated_at

    return ExportSummary(
        document_count=document_count,
        max_updated_at=max_updated_at,
    )


def build_manifest(
    dag_id: str,
    run_id: str,
    logical_date: datetime,
    database: str,
    results: list[dict[str, Any]],
) -> str:
    """Serialize the audit manifest written after all collections succeed."""
    payload = {
        "dag_id": dag_id,
        "run_id": run_id,
        "logical_date": format_checkpoint(logical_date),
        "source": {
            "type": "mongodb",
            "database": database,
        },
        "layer": "landing",
        "format": "mongodb_extended_json_canonical_lines",
        "total_documents": sum(item["document_count"] for item in results),
        "collections": sorted(results, key=lambda item: item["collection"]),
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def _extended_json_dumps(document: Mapping[str, Any]) -> str:
    from bson import json_util

    return json_util.dumps(document, json_options=json_util.CANONICAL_JSON_OPTIONS)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_component(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    if not normalized:
        raise ValueError("path component cannot be empty")
    return normalized
