from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "dags"))

from lib.mongodb_landing import (  # noqa: E402
    DEFAULT_COLLECTIONS,
    build_data_key,
    build_incremental_filter,
    build_manifest,
    build_manifest_key,
    checkpoint_variable_name,
    format_checkpoint,
    parse_checkpoint,
    parse_collections,
    write_json_lines,
)


class MongoDBLandingHelpersTest(unittest.TestCase):
    def test_default_collections_match_source_model(self):
        self.assertEqual(10, len(DEFAULT_COLLECTIONS))
        self.assertEqual(DEFAULT_COLLECTIONS, parse_collections(None))

    def test_collection_parser_rejects_duplicates(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            parse_collections("clientes,pedidos,clientes")

    def test_checkpoint_round_trip_is_utc(self):
        checkpoint = parse_checkpoint("2026-06-13T09:30:00-03:00")

        self.assertEqual(
            datetime(2026, 6, 13, 12, 30, tzinfo=timezone.utc),
            checkpoint,
        )
        self.assertEqual("2026-06-13T12:30:00Z", format_checkpoint(checkpoint))

    def test_incremental_filter_applies_overlap_window(self):
        checkpoint = datetime(2026, 6, 13, 12, tzinfo=timezone.utc)

        query = build_incremental_filter(checkpoint, "updated_at", 24)

        self.assertEqual(
            {
                "updated_at": {
                    "$gte": datetime(
                        2026,
                        6,
                        12,
                        12,
                        tzinfo=timezone.utc,
                    )
                }
            },
            query,
        )
        self.assertEqual({}, build_incremental_filter(None, "updated_at", 24))

    def test_incremental_filter_rejects_negative_overlap(self):
        with self.assertRaisesRegex(ValueError, "negative"):
            build_incremental_filter(
                datetime(2026, 6, 13, tzinfo=timezone.utc),
                "updated_at",
                -1,
            )

    def test_keys_are_partitioned_and_sanitized(self):
        logical_date = datetime(2026, 6, 13, 10, tzinfo=timezone.utc)

        data_key = build_data_key(
            "ecommerce",
            "itens_pedido",
            logical_date,
            "manual__2026-06-13T10:00:00+00:00",
        )
        manifest_key = build_manifest_key(
            logical_date,
            "manual__2026-06-13T10:00:00+00:00",
        )

        self.assertEqual(
            "landing/ecommerce/itens_pedido/"
            "extraction_date=2026-06-13/"
            "run_id=manual__2026-06-13T10_00_00_00_00/"
            "part-00000.json",
            data_key,
        )
        self.assertTrue(manifest_key.endswith("/manifest.json"))
        self.assertNotIn(":", manifest_key)

    def test_checkpoint_variable_name_is_scoped_by_collection(self):
        self.assertEqual(
            "mongodb_landing_checkpoint__ecommerce_itens_pedido",
            checkpoint_variable_name("ecommerce", "itens_pedido"),
        )

    def test_json_lines_are_streamed_and_checkpointed(self):
        documents = [
            {
                "_id": "a",
                "updated_at": datetime(2026, 6, 11),
            },
            {
                "_id": "b",
                "updated_at": datetime(
                    2026,
                    6,
                    13,
                    tzinfo=timezone.utc,
                ),
            },
        ]

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "part-00000.json"
            summary = write_json_lines(
                documents,
                output,
                "updated_at",
                serializer=lambda item: json.dumps(item, default=str),
            )
            lines = output.read_text(encoding="utf-8").splitlines()

        self.assertEqual(2, summary.document_count)
        self.assertEqual(
            datetime(2026, 6, 13, tzinfo=timezone.utc),
            summary.max_updated_at,
        )
        self.assertEqual(2, len(lines))
        self.assertEqual("a", json.loads(lines[0])["_id"])

    def test_canonical_extended_json_preserves_bson_types(self):
        try:
            from bson import ObjectId
        except ImportError:
            self.skipTest("PyMongo is installed with the Airflow requirements")

        document = {
            "_id": ObjectId("665000000000000000000001"),
            "id_cliente": 7,
            "updated_at": datetime(
                2026,
                6,
                13,
                tzinfo=timezone.utc,
            ),
        }

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "part-00000.json"
            write_json_lines([document], output, "updated_at")
            exported = json.loads(output.read_text(encoding="utf-8"))

        self.assertEqual(
            "665000000000000000000001",
            exported["_id"]["$oid"],
        )
        self.assertEqual("7", exported["id_cliente"]["$numberInt"])
        self.assertIn("$numberLong", exported["updated_at"]["$date"])

    def test_manifest_contains_auditable_totals(self):
        manifest = json.loads(
            build_manifest(
                dag_id="mongodb_to_landing",
                run_id="manual__test",
                logical_date=datetime(
                    2026,
                    6,
                    13,
                    tzinfo=timezone.utc,
                ),
                database="ecommerce",
                results=[
                    {
                        "collection": "pedidos",
                        "document_count": 3,
                        "object_key": "landing/pedidos.json",
                        "checkpoint_variable": "checkpoint_pedidos",
                        "checkpoint_before": None,
                        "checkpoint_after": "2026-06-13T00:00:00Z",
                    },
                    {
                        "collection": "clientes",
                        "document_count": 2,
                        "object_key": "landing/clientes.json",
                        "checkpoint_variable": "checkpoint_clientes",
                        "checkpoint_before": None,
                        "checkpoint_after": "2026-06-13T00:00:00Z",
                    },
                ],
            )
        )

        self.assertEqual(5, manifest["total_documents"])
        self.assertEqual(
            ["clientes", "pedidos"],
            [item["collection"] for item in manifest["collections"]],
        )
        self.assertEqual(
            "mongodb_extended_json_canonical_lines",
            manifest["format"],
        )


if __name__ == "__main__":
    unittest.main()
