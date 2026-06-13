from __future__ import annotations

import importlib.util
import io
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "criar_estrutura_landing.py"
SPEC = importlib.util.spec_from_file_location("criar_estrutura_landing", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = MODULE
SPEC.loader.exec_module(MODULE)

from dags.lib.mongodb_landing import DEFAULT_COLLECTIONS  # noqa: E402


LandingConfig = MODULE.LandingConfig
build_structure_manifest = MODULE.build_structure_manifest
collection_marker_key = MODULE.collection_marker_key
initialize_landing = MODULE.initialize_landing
load_config = MODULE.load_config
structure_manifest_key = MODULE.structure_manifest_key
validate_landing = MODULE.validate_landing


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


class LandingStructureTest(unittest.TestCase):
    def setUp(self):
        self.config = load_config(REPO_ROOT / "config" / "landing_structure.json")
        self.client = FakeS3Client()

    def test_contract_matches_mongodb_to_landing_dag(self):
        self.assertEqual("datalake", self.config.bucket)
        self.assertEqual("ecommerce", self.config.database)
        self.assertEqual("landing", self.config.layer)
        self.assertEqual(DEFAULT_COLLECTIONS, self.config.collections)

    def test_load_config_rejects_duplicate_collections(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "landing.json"
            path.write_text(
                json.dumps(
                    {
                        "bucket": "datalake",
                        "database": "ecommerce",
                        "layer": "landing",
                        "collections": ["clientes", "clientes"],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate"):
                load_config(path)

    def test_initialize_creates_bucket_markers_manifest_and_versioning(self):
        result = initialize_landing(
            self.client,
            self.config,
            generated_at=datetime(2026, 6, 13, tzinfo=timezone.utc),
        )

        self.assertTrue(result["bucket_created"])
        self.assertEqual(10, result["collection_count"])
        self.assertEqual(10, len(result["markers_created"]))
        self.assertTrue(result["manifest_updated"])
        self.assertEqual("Enabled", self.client.versioning["datalake"])
        self.assertEqual([], validate_landing(self.client, self.config))

        objects = self.client.buckets["datalake"]
        self.assertIn(
            collection_marker_key(self.config, "clientes"),
            objects,
        )
        self.assertIn(structure_manifest_key(self.config), objects)

    def test_initialize_is_idempotent(self):
        first = initialize_landing(self.client, self.config)
        keys_after_first_run = set(self.client.buckets["datalake"])
        puts_after_first_run = self.client.put_count

        second = initialize_landing(self.client, self.config)
        keys_after_second_run = set(self.client.buckets["datalake"])

        self.assertTrue(first["bucket_created"])
        self.assertFalse(second["bucket_created"])
        self.assertEqual([], second["markers_created"])
        self.assertFalse(second["manifest_updated"])
        self.assertEqual(keys_after_first_run, keys_after_second_run)
        self.assertEqual(puts_after_first_run, self.client.put_count)

    def test_validation_reports_missing_required_object(self):
        initialize_landing(self.client, self.config)
        missing_key = collection_marker_key(self.config, "pedidos")
        del self.client.buckets["datalake"][missing_key]

        self.assertEqual(
            [f"s3://datalake/{missing_key}"],
            validate_landing(self.client, self.config),
        )

    def test_manifest_documents_all_collection_prefixes(self):
        manifest = json.loads(
            build_structure_manifest(
                self.config,
                datetime(2026, 6, 13, tzinfo=timezone.utc),
            )
        )

        self.assertEqual(
            "mongodb_extended_json_canonical_lines",
            manifest["format"],
        )
        self.assertEqual("2026-06-13T00:00:00Z", manifest["generated_at"])
        self.assertEqual(10, len(manifest["collections"]))
        self.assertEqual(
            "landing/ecommerce/itens_pedido/",
            next(
                item["prefix"]
                for item in manifest["collections"]
                if item["name"] == "itens_pedido"
            ),
        )


if __name__ == "__main__":
    unittest.main()
