"""Contracts and pure helpers for the Silver to Gold pipeline."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

try:
    from .landing_bronze import (
        build_spark_conf,
        parse_logical_date,
        spark_packages,
    )
except ImportError:  # Airflow may parse helper modules as standalone files
    from lib.landing_bronze import (
        build_spark_conf,
        parse_logical_date,
        spark_packages,
    )


SILVER_TABLES = (
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


# --- Colunas de controle e sentinelas do SCD Tipo 2 -------------------------
SCD2_VALID_FROM = "dw_valid_from"
SCD2_VALID_TO = "dw_valid_to"
SCD2_IS_CURRENT = "dw_is_current"
SCD2_RECORD_HASH = "dw_record_hash"
SCD2_CONTROL_COLUMNS = (
    SCD2_VALID_FROM,
    SCD2_VALID_TO,
    SCD2_IS_CURRENT,
    SCD2_RECORD_HASH,
)
# Início "desde sempre" da primeira versão (faz os fatos históricos casarem)
# e fim em aberto da versão vigente.
SCD2_BEGINNING_OF_TIME = "1900-01-01 00:00:00"
SCD2_END_OF_TIME = "9999-12-31 23:59:59"


@dataclass(frozen=True)
class GoldModelRule:
    primary_key: str
    kind: str
    source_tables: tuple[str, ...]
    partition_columns: tuple[str, ...] = ()
    # "type2" -> dimensão versionada; "static" -> dimensão sem histórico
    # (ex.: calendário); "none" -> fato.
    scd_type: str = "none"
    # Chave natural/durável de negócio (estável entre versões).
    natural_key: str | None = None
    # Chave substituta única por versão (PK da dimensão SCD2).
    surrogate_key: str | None = None


GOLD_MODELS: dict[str, GoldModelRule] = {
    "dim_tempo": GoldModelRule(
        primary_key="data_key",
        kind="dimension",
        source_tables=(
            "clientes",
            "cupons",
            "pedidos",
            "pagamentos",
            "entregas",
            "avaliacoes",
        ),
        scd_type="static",
    ),
    "dim_cliente": GoldModelRule(
        primary_key="cliente_sk",
        kind="dimension",
        source_tables=("clientes",),
        scd_type="type2",
        natural_key="cliente_key",
        surrogate_key="cliente_sk",
    ),
    "dim_produto": GoldModelRule(
        primary_key="produto_sk",
        kind="dimension",
        source_tables=("produtos", "categorias", "fornecedores"),
        scd_type="type2",
        natural_key="produto_key",
        surrogate_key="produto_sk",
    ),
    "dim_cupom": GoldModelRule(
        primary_key="cupom_sk",
        kind="dimension",
        source_tables=("cupons",),
        scd_type="type2",
        natural_key="cupom_key",
        surrogate_key="cupom_sk",
    ),
    "fato_vendas": GoldModelRule(
        primary_key="venda_key",
        kind="fact",
        source_tables=("itens_pedido", "pedidos"),
        partition_columns=("ano",),
    ),
    "fato_pagamentos": GoldModelRule(
        primary_key="pagamento_key",
        kind="fact",
        source_tables=("pagamentos", "pedidos"),
        partition_columns=("ano",),
    ),
    "fato_entregas": GoldModelRule(
        primary_key="entrega_key",
        kind="fact",
        source_tables=("entregas", "pedidos"),
        partition_columns=("ano",),
    ),
    "fato_avaliacoes": GoldModelRule(
        primary_key="avaliacao_key",
        kind="fact",
        source_tables=("avaliacoes",),
        partition_columns=("ano",),
    ),
}


def parse_gold_models(raw_value: str | None) -> tuple[str, ...]:
    """Parse a model selection and restore the dimensional dependency order."""
    if not raw_value:
        return tuple(GOLD_MODELS)

    models = tuple(name.strip() for name in raw_value.split(",") if name.strip())
    if not models:
        raise ValueError("GOLD_TABLES must contain at least one model")
    if len(models) != len(set(models)):
        raise ValueError("GOLD_TABLES contains duplicate model names")

    unknown = sorted(set(models) - set(GOLD_MODELS))
    if unknown:
        raise ValueError("Unknown Gold models: " + ", ".join(unknown))
    selected = set(models)
    return tuple(model for model in GOLD_MODELS if model in selected)


def scd2_models() -> tuple[str, ...]:
    """Modelos cujas dimensões usam SCD Tipo 2 (versionadas)."""
    return tuple(
        name for name, rule in GOLD_MODELS.items() if rule.scd_type == "type2"
    )


def is_scd2(model: str) -> bool:
    return GOLD_MODELS[model].scd_type == "type2"


def silver_table_uri(
    bucket: str,
    database: str,
    table: str,
    scheme: str = "s3a",
) -> str:
    return (
        f"{scheme}://{_safe_component(bucket)}/silver/"
        f"{_safe_component(database)}/{_safe_component(table)}/"
    )


def gold_table_uri(
    bucket: str,
    database: str,
    table: str,
    scheme: str = "s3a",
) -> str:
    return (
        f"{scheme}://{_safe_component(bucket)}/gold/"
        f"{_safe_component(database)}/{_safe_component(table)}/"
    )


def build_manifest_key(logical_date: datetime, run_id: str) -> str:
    processing_date = _as_utc(logical_date).date().isoformat()
    return (
        "gold/_control/silver_to_gold/"
        f"processing_date={processing_date}/"
        f"run_id={_safe_component(run_id)}/manifest.json"
    )


def build_manifest(
    *,
    run_id: str,
    logical_date: datetime,
    database: str,
    results: Sequence[Mapping[str, Any]],
) -> str:
    ordered = sorted(
        results,
        key=lambda item: tuple(GOLD_MODELS).index(str(item["table"])),
    )
    payload = {
        "dag_id": "silver_to_gold",
        "run_id": run_id,
        "logical_date": _as_utc(logical_date).isoformat().replace("+00:00", "Z"),
        "source": {"layer": "silver", "format": "delta"},
        "target": {
            "layer": "gold",
            "format": "delta",
            "database": database,
            "model": "dimensional",
        },
        "totals": {
            key: sum(int(item.get(key, 0)) for item in ordered)
            for key in (
                "records_modelled",
                "inserted",
                "updated",
                "deleted",
                "unchanged",
                "versions_expired",
                "versions_inserted",
                "rows_written",
            )
        },
        "tables": ordered,
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _safe_component(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    if not normalized:
        raise ValueError("path component cannot be empty")
    return normalized


__all__ = [
    "GOLD_MODELS",
    "SILVER_TABLES",
    "SCD2_VALID_FROM",
    "SCD2_VALID_TO",
    "SCD2_IS_CURRENT",
    "SCD2_RECORD_HASH",
    "SCD2_CONTROL_COLUMNS",
    "SCD2_BEGINNING_OF_TIME",
    "SCD2_END_OF_TIME",
    "GoldModelRule",
    "build_manifest",
    "build_manifest_key",
    "build_spark_conf",
    "gold_table_uri",
    "is_scd2",
    "parse_gold_models",
    "parse_logical_date",
    "scd2_models",
    "silver_table_uri",
    "spark_packages",
]
