"""Contracts and pure helpers for the Bronze to Silver pipeline."""

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


@dataclass(frozen=True)
class FieldRule:
    name: str
    source_type: str
    target_type: str
    required: bool = True
    normalize: str | None = None
    allowed_values: tuple[str, ...] = ()
    pattern: str | None = None
    minimum: float | None = None
    maximum: float | None = None


@dataclass(frozen=True)
class ForeignKeyRule:
    field: str
    target_table: str
    target_field: str
    required: bool = True


@dataclass(frozen=True)
class UniqueRule:
    """Regra de unicidade de negócio aplicada após a dedup por chave primária.

    Se *condition_field* e *condition_value* estiverem preenchidos, a regra
    aplica-se apenas ao subconjunto de linhas em que
    ``condition_field == condition_value`` (ex.: só pagamentos aprovados).
    Para linhas fora da condição o registro passa sem restrição.
    """

    fields: tuple[str, ...]
    rejection_type: str
    description: str
    condition_field: str | None = None
    condition_value: str | None = None


@dataclass(frozen=True)
class EntityRule:
    primary_key: str
    fields: tuple[FieldRule, ...]
    foreign_keys: tuple[ForeignKeyRule, ...] = ()
    unique_rules: tuple[UniqueRule, ...] = ()


def _field(
    name: str,
    source_type: str,
    target_type: str,
    **kwargs,
) -> FieldRule:
    return FieldRule(name, source_type, target_type, **kwargs)


ENTITY_RULES: dict[str, EntityRule] = {
    "clientes": EntityRule(
        primary_key="id_cliente",
        fields=(
            _field("id_cliente", "integer", "long", minimum=1),
            _field("nome", "string", "string", normalize="trim"),
            _field(
                "email",
                "string",
                "string",
                normalize="lower",
                pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
            ),
            _field(
                "cpf",
                "string",
                "string",
                normalize="digits",
                pattern=r"^\d{11}$",
            ),
            _field(
                "telefone",
                "string",
                "string",
                normalize="digits",
                pattern=r"^\d{10,13}$",
            ),
            _field("data_nascimento", "date", "date"),
            _field(
                "genero",
                "string",
                "string",
                normalize="trim",
                allowed_values=("M", "F", "Outro"),
            ),
            _field("logradouro", "string", "string", normalize="trim"),
            _field("cidade", "string", "string", normalize="trim"),
            _field(
                "estado",
                "string",
                "string",
                normalize="upper",
                pattern=r"^[A-Z]{2}$",
            ),
            _field(
                "cep",
                "string",
                "string",
                normalize="digits",
                pattern=r"^\d{8}$",
            ),
            _field("data_cadastro", "date", "date"),
            _field("updated_at", "date", "timestamp"),
        ),
        unique_rules=(
            UniqueRule(
                fields=("cpf",),
                rejection_type="cpf_duplicado",
                description=(
                    "CPF deve ser unico entre clientes — "
                    "mantido o registro com updated_at mais recente"
                ),
            ),
        ),
    ),
    "categorias": EntityRule(
        primary_key="id_categoria",
        fields=(
            _field("id_categoria", "integer", "long", minimum=1),
            _field("nome_categoria", "string", "string", normalize="trim"),
            _field("descricao", "string", "string", normalize="trim"),
            _field("updated_at", "date", "timestamp"),
        ),
    ),
    "fornecedores": EntityRule(
        primary_key="id_fornecedor",
        fields=(
            _field("id_fornecedor", "integer", "long", minimum=1),
            _field("nome_fornecedor", "string", "string", normalize="trim"),
            _field(
                "cnpj",
                "string",
                "string",
                normalize="digits",
                pattern=r"^\d{14}$",
            ),
            _field(
                "email",
                "string",
                "string",
                normalize="lower",
                pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$",
            ),
            _field(
                "telefone",
                "string",
                "string",
                normalize="digits",
                pattern=r"^\d{10,13}$",
            ),
            _field("logradouro", "string", "string", normalize="trim"),
            _field("cidade", "string", "string", normalize="trim"),
            _field(
                "estado",
                "string",
                "string",
                normalize="upper",
                pattern=r"^[A-Z]{2}$",
            ),
            _field(
                "cep",
                "string",
                "string",
                normalize="digits",
                pattern=r"^\d{8}$",
            ),
            _field("updated_at", "date", "timestamp"),
        ),
    ),
    "produtos": EntityRule(
        primary_key="id_produto",
        fields=(
            _field("id_produto", "integer", "long", minimum=1),
            _field("nome_produto", "string", "string", normalize="trim"),
            _field("descricao", "string", "string", normalize="trim"),
            _field("preco", "number", "decimal(18,2)", minimum=0),
            _field("estoque", "integer", "long", minimum=0),
            _field("id_categoria", "integer", "long", minimum=1),
            _field("id_fornecedor", "integer", "long", minimum=1),
            _field("marca", "string", "string", normalize="trim"),
            _field("peso_kg", "number", "decimal(10,2)", minimum=0),
            _field("updated_at", "date", "timestamp"),
        ),
        foreign_keys=(
            ForeignKeyRule("id_categoria", "categorias", "id_categoria"),
            ForeignKeyRule("id_fornecedor", "fornecedores", "id_fornecedor"),
        ),
    ),
    "cupons": EntityRule(
        primary_key="id_cupom",
        fields=(
            _field("id_cupom", "integer", "long", minimum=1),
            _field("codigo", "string", "string", normalize="upper"),
            _field(
                "desconto_percentual",
                "integer",
                "long",
                minimum=0,
                maximum=100,
            ),
            _field("valor_minimo", "integer", "long", minimum=0),
            _field("data_validade", "date", "date"),
            _field("ativo", "boolean", "boolean"),
            _field("updated_at", "date", "timestamp"),
        ),
    ),
    "pedidos": EntityRule(
        primary_key="id_pedido",
        fields=(
            _field("id_pedido", "integer", "long", minimum=1),
            _field("id_cliente", "integer", "long", minimum=1),
            _field("data_pedido", "date", "date"),
            _field(
                "status",
                "string",
                "string",
                normalize="lower",
                allowed_values=(
                    "pendente",
                    "processando",
                    "enviado",
                    "entregue",
                    "cancelado",
                ),
            ),
            _field("valor_total", "number", "decimal(18,2)", minimum=0),
            _field(
                "id_cupom",
                "integer",
                "long",
                required=False,
                minimum=1,
            ),
            _field("updated_at", "date", "timestamp"),
        ),
        foreign_keys=(
            ForeignKeyRule("id_cliente", "clientes", "id_cliente"),
            ForeignKeyRule(
                "id_cupom",
                "cupons",
                "id_cupom",
                required=False,
            ),
        ),
    ),
    "itens_pedido": EntityRule(
        primary_key="id_item",
        fields=(
            _field("id_item", "integer", "long", minimum=1),
            _field("id_pedido", "integer", "long", minimum=1),
            _field("id_produto", "integer", "long", minimum=1),
            _field("quantidade", "integer", "long", minimum=1),
            _field("valor_unitario", "number", "decimal(18,2)", minimum=0),
            _field(
                "desconto_percentual",
                "number",
                "decimal(5,2)",
                minimum=0,
                maximum=100,
            ),
            _field("subtotal", "number", "decimal(18,2)", minimum=0),
            _field("updated_at", "date", "timestamp"),
        ),
        foreign_keys=(
            ForeignKeyRule("id_pedido", "pedidos", "id_pedido"),
            ForeignKeyRule("id_produto", "produtos", "id_produto"),
        ),
    ),
    "pagamentos": EntityRule(
        primary_key="id_pagamento",
        fields=(
            _field("id_pagamento", "integer", "long", minimum=1),
            _field("id_pedido", "integer", "long", minimum=1),
            _field(
                "forma_pagamento",
                "string",
                "string",
                normalize="lower",
                allowed_values=(
                    "cartao_credito",
                    "cartao_debito",
                    "pix",
                    "boleto",
                ),
            ),
            _field(
                "status_pagamento",
                "string",
                "string",
                normalize="lower",
                allowed_values=(
                    "aprovado",
                    "pendente",
                    "recusado",
                    "estornado",
                ),
            ),
            _field("valor", "number", "decimal(18,2)", minimum=0),
            _field("data_pagamento", "date", "date"),
            _field("parcelas", "integer", "long", minimum=1, maximum=12),
            _field("updated_at", "date", "timestamp"),
        ),
        foreign_keys=(ForeignKeyRule("id_pedido", "pedidos", "id_pedido"),),
        unique_rules=(
            UniqueRule(
                fields=("id_pedido",),
                rejection_type="pagamento_duplicado_aprovado",
                description=(
                    "Cada pedido pode ter no maximo um pagamento com "
                    "status aprovado — mantido o registro com updated_at mais recente"
                ),
                condition_field="status_pagamento",
                condition_value="aprovado",
            ),
        ),
    ),
    "entregas": EntityRule(
        primary_key="id_entrega",
        fields=(
            _field("id_entrega", "integer", "long", minimum=1),
            _field("id_pedido", "integer", "long", minimum=1),
            _field(
                "status_entrega",
                "string",
                "string",
                normalize="lower",
                allowed_values=(
                    "pendente",
                    "em_transito",
                    "entregue",
                    "devolvido",
                ),
            ),
            _field("data_envio", "date", "date"),
            _field("data_entrega_prevista", "date", "date"),
            _field(
                "data_entrega_real",
                "date",
                "date",
                required=False,
            ),
            _field(
                "transportadora",
                "string",
                "string",
                normalize="trim",
                allowed_values=(
                    "Correios",
                    "JadLog",
                    "Total Express",
                    "Loggi",
                    "Azul Cargo",
                    "Latam Cargo",
                    "DHL",
                    "FedEx",
                ),
            ),
            _field(
                "codigo_rastreio",
                "string",
                "string",
                normalize="upper",
                pattern=r"^[A-Z]{2}\d{9}BR$",
            ),
            _field("updated_at", "date", "timestamp"),
        ),
        foreign_keys=(ForeignKeyRule("id_pedido", "pedidos", "id_pedido"),),
    ),
    "avaliacoes": EntityRule(
        primary_key="id_avaliacao",
        fields=(
            _field("id_avaliacao", "integer", "long", minimum=1),
            _field("id_pedido", "integer", "long", minimum=1),
            _field("id_cliente", "integer", "long", minimum=1),
            _field("id_produto", "integer", "long", minimum=1),
            _field("nota", "integer", "long", minimum=1, maximum=5),
            _field("comentario", "string", "string", normalize="trim"),
            _field("data_avaliacao", "date", "date"),
            _field("updated_at", "date", "timestamp"),
        ),
        foreign_keys=(
            ForeignKeyRule("id_pedido", "pedidos", "id_pedido"),
            ForeignKeyRule("id_cliente", "clientes", "id_cliente"),
            ForeignKeyRule("id_produto", "produtos", "id_produto"),
        ),
    ),
}


def parse_pipeline_tables(raw_value: str | None) -> tuple[str, ...]:
    """Parse and validate an ordered table list."""
    if not raw_value:
        return tuple(ENTITY_RULES)

    tables = tuple(name.strip() for name in raw_value.split(",") if name.strip())
    if not tables:
        raise ValueError("SILVER_TABLES must contain at least one table")
    if len(tables) != len(set(tables)):
        raise ValueError("SILVER_TABLES contains duplicate table names")

    unknown = sorted(set(tables) - set(ENTITY_RULES))
    if unknown:
        raise ValueError("Unknown Silver tables: " + ", ".join(unknown))

    selected = set(tables)
    for table in tables:
        missing_dependencies = sorted(
            {
                foreign_key.target_table
                for foreign_key in ENTITY_RULES[table].foreign_keys
                if foreign_key.target_table not in selected
            }
        )
        if missing_dependencies:
            raise ValueError(
                f"Table {table!r} requires Silver dependencies: "
                + ", ".join(missing_dependencies)
            )
    return tuple(table for table in ENTITY_RULES if table in selected)


def bronze_table_uri(
    bucket: str,
    database: str,
    table: str,
    scheme: str = "s3a",
) -> str:
    return (
        f"{scheme}://{_safe_component(bucket)}/bronze/"
        f"{_safe_component(database)}/{_safe_component(table)}/"
    )


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


def build_manifest_key(logical_date: datetime, run_id: str) -> str:
    processing_date = _as_utc(logical_date).date().isoformat()
    return (
        "silver/_control/bronze_to_silver/"
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
    ordered = sorted(results, key=lambda item: str(item["table"]))
    payload = {
        "dag_id": "bronze_to_silver",
        "run_id": run_id,
        "logical_date": _as_utc(logical_date).isoformat().replace("+00:00", "Z"),
        "source": {"layer": "bronze", "format": "delta"},
        "target": {
            "layer": "silver",
            "format": "delta",
            "database": database,
        },
        "totals": {
            key: sum(int(item[key]) for item in ordered)
            for key in (
                "records_read",
                "duplicates_removed",
                "field_rejected",
                "business_rule_rejected",
                "referential_rejected",
                "records_rejected",
                "records_valid",
                "inserted",
                "updated",
                "unchanged",
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


def apply_unique_rules(
    data,
    *,
    rule: "EntityRule",
    run_id: str,
    table: str,
):
    """Aplica regras de unicidade de negócio após a dedup por chave primária.

    Retorna ``(valid, rejected_list)`` onde *rejected_list* é uma lista de
    DataFrames com as colunas extras ``_rejection_type`` e
    ``_rejection_detail``, prontos para gravação no quality log.

    Critério de desempate (quem fica): ``updated_at`` mais recente →
    ``_silver_source_ingested_at`` mais recente → ``_silver_source_file``
    como critério determinístico final.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    if not rule.unique_rules:
        return data, []

    valid = data
    rejected_list: list = []

    for unique_rule in rule.unique_rules:
        if unique_rule.condition_field is not None:
            mask = F.col(unique_rule.condition_field) == unique_rule.condition_value
            subject = valid.filter(mask)
            remainder = valid.filter(~mask)
        else:
            subject = valid
            remainder = None

        window = Window.partitionBy(*unique_rule.fields).orderBy(
            F.col("updated_at").desc_nulls_last(),
            F.col("_silver_source_ingested_at").desc_nulls_last(),
            F.col("_silver_source_file").desc_nulls_last(),
        )
        ranked = subject.withColumn("_biz_rank", F.row_number().over(window))

        kept = ranked.filter(F.col("_biz_rank") == 1).drop("_biz_rank")
        rejected = (
            ranked.filter(F.col("_biz_rank") > 1)
            .drop("_biz_rank")
            .withColumn("_rejection_type", F.lit(unique_rule.rejection_type))
            .withColumn("_rejection_detail", F.lit(unique_rule.description))
        )
        rejected_list.append(rejected)

        if remainder is not None:
            valid = kept.unionByName(remainder)
        else:
            valid = kept

    return valid, rejected_list


def quality_log_uri(bucket: str, scheme: str = "s3a") -> str:
    """URI da tabela Delta de log de qualidade (append-only, Silver control)."""
    return f"{scheme}://{_safe_component(bucket)}/silver/_control/quality_log/"


def _safe_component(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    if not normalized:
        raise ValueError("path component cannot be empty")
    return normalized


__all__ = [
    "ENTITY_RULES",
    "EntityRule",
    "FieldRule",
    "ForeignKeyRule",
    "UniqueRule",
    "apply_unique_rules",
    "bronze_table_uri",
    "build_manifest",
    "build_manifest_key",
    "build_spark_conf",
    "parse_logical_date",
    "parse_pipeline_tables",
    "quality_log_uri",
    "silver_table_uri",
    "spark_packages",
]
