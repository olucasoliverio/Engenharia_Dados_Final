from __future__ import annotations

import io
import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from dags.lib.mongodb_landing import DEFAULT_COLLECTIONS
from scripts.criar_estrutura_silver import (
    build_structure_manifest,
    delta_log_prefix,
    initialize_silver,
    load_config,
    structure_manifest_key,
    table_marker_key,
    validate_silver,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


class FakeClientError(Exception):
    def __init__(self, code: str):
        super().__init__(code)
        self.response = {"Error": {"Code": code}}


class FakeS3Client:
    def __init__(self):
        self.buckets: dict[str, dict[str, bytes]] = {}
        self.versioning: dict[str, str] = {}
        self.put_count = 0

    def head_bucket(self, Bucket):
        if Bucket not in self.buckets:
            raise FakeClientError("NoSuchBucket")

    def create_bucket(self, Bucket, **kwargs):
        self.buckets[Bucket] = {}

    def put_bucket_versioning(self, Bucket, VersioningConfiguration):
        self.versioning[Bucket] = VersioningConfiguration["Status"]

    def put_object(self, Bucket, Key, Body, **kwargs):
        self.buckets[Bucket][Key] = Body
        self.put_count += 1

    def head_object(self, Bucket, Key):
        if Key not in self.buckets.get(Bucket, {}):
            raise FakeClientError("NoSuchKey")

    def get_object(self, Bucket, Key):
        if Key not in self.buckets.get(Bucket, {}):
            raise FakeClientError("NoSuchKey")
        return {"Body": io.BytesIO(self.buckets[Bucket][Key])}


class SilverStructureTest(unittest.TestCase):
    def setUp(self):
        self.config = load_config(REPO_ROOT / "config" / "silver_structure.json")
        self.client = FakeS3Client()

    def test_contract_matches_granular_bronze_tables(self):
        self.assertEqual("datalake", self.config.bucket)
        self.assertEqual("ecommerce", self.config.database)
        self.assertEqual("silver", self.config.layer)
        self.assertEqual(DEFAULT_COLLECTIONS, self.config.tables)
        self.assertEqual((), self.config.partition_columns)
        self.assertEqual("bronze", self.config.source_layer)
        self.assertEqual("delta", self.config.source_format)

    def test_load_config_rejects_wrong_layer_or_duplicate_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "silver.json"
            payload = {
                "bucket": "datalake",
                "database": "ecommerce",
                "layer": "bronze",
                "tables": ["clientes"],
                "partition_columns": [],
            }
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "must be 'silver'"):
                load_config(path)

            payload["layer"] = "silver"
            payload["tables"] = ["clientes", "clientes"]
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "duplicate"):
                load_config(path)

    def test_initialize_creates_markers_manifest_and_versioning(self):
        result = initialize_silver(
            self.client,
            self.config,
            generated_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
        )

        self.assertTrue(result["bucket_created"])
        self.assertEqual(10, result["table_count"])
        self.assertEqual(10, len(result["markers_created"]))
        self.assertTrue(result["manifest_updated"])
        self.assertEqual("Enabled", self.client.versioning["datalake"])
        self.assertEqual([], validate_silver(self.client, self.config))

        objects = self.client.buckets["datalake"]
        self.assertIn(table_marker_key(self.config, "clientes"), objects)
        self.assertIn(structure_manifest_key(self.config), objects)

    def test_initialize_is_idempotent(self):
        first = initialize_silver(self.client, self.config)
        keys_after_first_run = set(self.client.buckets["datalake"])
        puts_after_first_run = self.client.put_count

        second = initialize_silver(self.client, self.config)

        self.assertTrue(first["bucket_created"])
        self.assertFalse(second["bucket_created"])
        self.assertEqual([], second["markers_created"])
        self.assertFalse(second["manifest_updated"])
        self.assertEqual(keys_after_first_run, set(self.client.buckets["datalake"]))
        self.assertEqual(puts_after_first_run, self.client.put_count)

    def test_validation_reports_missing_marker_and_stale_manifest(self):
        initialize_silver(self.client, self.config)
        marker = table_marker_key(self.config, "pedidos")
        del self.client.buckets["datalake"][marker]

        manifest_key = structure_manifest_key(self.config)
        manifest = json.loads(self.client.buckets["datalake"][manifest_key])
        manifest["source"]["layer"] = "landing"
        self.client.buckets["datalake"][manifest_key] = json.dumps(manifest).encode()

        self.assertEqual(
            [
                f"s3://datalake/{marker}",
                f"s3://datalake/{manifest_key} (divergente)",
            ],
            validate_silver(self.client, self.config),
        )

    def test_manifest_documents_silver_delta_contract(self):
        manifest = json.loads(
            build_structure_manifest(
                self.config,
                datetime(2026, 6, 13, tzinfo=timezone.utc),
            )
        )

        self.assertEqual("delta", manifest["format"])
        self.assertEqual({"layer": "bronze", "format": "delta"}, manifest["source"])
        self.assertEqual([], manifest["partition_columns"])
        self.assertEqual(10, len(manifest["tables"]))
        self.assertEqual(
            "silver/ecommerce/itens_pedido/_delta_log/",
            delta_log_prefix(self.config, "itens_pedido"),
        )


if __name__ == "__main__":
    unittest.main()
