from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone

from dags.lib.bronze_silver import (
    ENTITY_RULES,
    bronze_table_uri,
    build_manifest,
    build_manifest_key,
    parse_pipeline_tables,
    silver_table_uri,
)


class BronzeSilverHelpersTest(unittest.TestCase):
    def test_contract_defines_ten_entities_in_dependency_order(self):
        tables = parse_pipeline_tables(None)

        self.assertEqual(10, len(tables))
        self.assertLess(tables.index("clientes"), tables.index("pedidos"))
        self.assertLess(tables.index("pedidos"), tables.index("pagamentos"))
        self.assertLess(tables.index("produtos"), tables.index("avaliacoes"))

    def test_table_parser_rejects_duplicates_unknown_and_missing_dependencies(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            parse_pipeline_tables("clientes,clientes")
        with self.assertRaisesRegex(ValueError, "Unknown"):
            parse_pipeline_tables("desconhecida")
        with self.assertRaisesRegex(ValueError, "requires Silver dependencies"):
            parse_pipeline_tables("pedidos")

    def test_table_parser_restores_dependency_order(self):
        tables = parse_pipeline_tables("produtos,fornecedores,categorias,clientes")

        self.assertEqual(
            ("clientes", "categorias", "fornecedores", "produtos"),
            tables,
        )

    def test_layer_uris_are_table_scoped(self):
        self.assertEqual(
            "s3a://datalake/bronze/ecommerce/pedidos/",
            bronze_table_uri("datalake", "ecommerce", "pedidos"),
        )
        self.assertEqual(
            "s3a://datalake/silver/ecommerce/pedidos/",
            silver_table_uri("datalake", "ecommerce", "pedidos"),
        )

    def test_rules_define_primary_keys_and_updated_at(self):
        for table, rule in ENTITY_RULES.items():
            field_names = {field.name for field in rule.fields}
            self.assertIn(rule.primary_key, field_names, table)
            self.assertIn("updated_at", field_names, table)

    def test_only_expected_business_fields_are_optional(self):
        optional = {
            (table, field.name)
            for table, rule in ENTITY_RULES.items()
            for field in rule.fields
            if not field.required
        }

        self.assertEqual(
            {
                ("pedidos", "id_cupom"),
                ("entregas", "data_entrega_real"),
            },
            optional,
        )

    def test_manifest_key_is_partitioned_and_sanitized(self):
        key = build_manifest_key(
            datetime(2026, 6, 13, 10, tzinfo=timezone.utc),
            "manual__2026-06-13T10:00:00+00:00",
        )

        self.assertEqual(
            "silver/_control/bronze_to_silver/"
            "processing_date=2026-06-13/"
            "run_id=manual__2026-06-13T10_00_00_00_00/"
            "manifest.json",
            key,
        )

    def test_manifest_contains_quality_and_merge_totals(self):
        result = {
            "table": "clientes",
            "records_read": 12,
            "duplicates_removed": 2,
            "records_rejected": 1,
            "records_valid": 9,
            "inserted": 7,
            "updated": 1,
            "unchanged": 1,
            "rows_written": 8,
        }
        manifest = json.loads(
            build_manifest(
                run_id="manual__test",
                logical_date=datetime(2026, 6, 13, tzinfo=timezone.utc),
                database="ecommerce",
                results=[result],
            )
        )

        self.assertEqual("delta", manifest["target"]["format"])
        self.assertEqual(2, manifest["totals"]["duplicates_removed"])
        self.assertEqual(1, manifest["totals"]["records_rejected"])
        self.assertEqual(8, manifest["totals"]["rows_written"])


if __name__ == "__main__":
    unittest.main()
