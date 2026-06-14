from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone

from dags.lib.silver_gold import (
    GOLD_MODELS,
    SILVER_TABLES,
    build_manifest,
    build_manifest_key,
    gold_table_uri,
    parse_gold_models,
    silver_table_uri,
)


class SilverGoldHelpersTest(unittest.TestCase):
    def test_contract_defines_four_dimensions_and_four_facts(self):
        dimensions = [
            name for name, rule in GOLD_MODELS.items() if rule.kind == "dimension"
        ]
        facts = [name for name, rule in GOLD_MODELS.items() if rule.kind == "fact"]

        self.assertEqual(4, len(dimensions))
        self.assertEqual(4, len(facts))
        self.assertEqual(
            (
                "dim_tempo",
                "dim_cliente",
                "dim_produto",
                "dim_cupom",
                "fato_vendas",
                "fato_pagamentos",
                "fato_entregas",
                "fato_avaliacoes",
            ),
            parse_gold_models(None),
        )

    def test_model_parser_rejects_invalid_values_and_restores_order(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            parse_gold_models("dim_tempo,dim_tempo")
        with self.assertRaisesRegex(ValueError, "Unknown"):
            parse_gold_models("fato_desconhecido")

        self.assertEqual(
            ("dim_cliente", "fato_vendas"),
            parse_gold_models("fato_vendas,dim_cliente"),
        )

    def test_fact_models_are_partitioned_by_year(self):
        for name, rule in GOLD_MODELS.items():
            expected = ("ano",) if rule.kind == "fact" else ()
            self.assertEqual(expected, rule.partition_columns, name)

    def test_all_model_sources_exist_in_silver_contract(self):
        for name, rule in GOLD_MODELS.items():
            self.assertTrue(rule.source_tables, name)
            self.assertTrue(set(rule.source_tables) <= set(SILVER_TABLES), name)

    def test_layer_uris_are_table_scoped(self):
        self.assertEqual(
            "s3a://datalake/silver/ecommerce/pedidos/",
            silver_table_uri("datalake", "ecommerce", "pedidos"),
        )
        self.assertEqual(
            "s3a://datalake/gold/ecommerce/fato_vendas/",
            gold_table_uri("datalake", "ecommerce", "fato_vendas"),
        )

    def test_manifest_key_is_partitioned_and_sanitized(self):
        key = build_manifest_key(
            datetime(2026, 6, 13, 10, tzinfo=timezone.utc),
            "manual__2026-06-13T10:00:00+00:00",
        )

        self.assertEqual(
            "gold/_control/silver_to_gold/"
            "processing_date=2026-06-13/"
            "run_id=manual__2026-06-13T10_00_00_00_00/"
            "manifest.json",
            key,
        )

    def test_manifest_contains_sync_totals(self):
        result = {
            "table": "dim_cliente",
            "records_modelled": 10,
            "inserted": 7,
            "updated": 1,
            "deleted": 1,
            "unchanged": 2,
            "rows_written": 9,
        }
        manifest = json.loads(
            build_manifest(
                run_id="manual__test",
                logical_date=datetime(2026, 6, 13, tzinfo=timezone.utc),
                database="ecommerce",
                results=[result],
            )
        )

        self.assertEqual("dimensional", manifest["target"]["model"])
        self.assertEqual(7, manifest["totals"]["inserted"])
        self.assertEqual(1, manifest["totals"]["deleted"])
        self.assertEqual(9, manifest["totals"]["rows_written"])


if __name__ == "__main__":
    unittest.main()
