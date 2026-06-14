from __future__ import annotations

import json
import unittest
from datetime import datetime, timezone

from dags.lib.landing_bronze import (
    bronze_table_uri,
    build_manifest,
    build_manifest_key,
    build_spark_conf,
    default_collections,
    landing_collection_uri,
    parse_logical_date,
    parse_pipeline_collections,
    spark_packages,
)


class LandingBronzeHelpersTest(unittest.TestCase):
    def test_collections_reuse_source_contract(self):
        self.assertEqual(10, len(default_collections()))
        self.assertEqual(default_collections(), parse_pipeline_collections(None))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            parse_pipeline_collections("clientes,pedidos,clientes")

    def test_layer_uris_are_s3a_and_collection_scoped(self):
        self.assertEqual(
            "s3a://datalake/landing/ecommerce/itens_pedido/",
            landing_collection_uri("datalake", "ecommerce", "itens_pedido"),
        )
        self.assertEqual(
            "s3a://datalake/bronze/ecommerce/itens_pedido/",
            bronze_table_uri("datalake", "ecommerce", "itens_pedido"),
        )

    def test_manifest_key_is_partitioned_and_sanitized(self):
        key = build_manifest_key(
            datetime(2026, 6, 13, 10, tzinfo=timezone.utc),
            "manual__2026-06-13T10:00:00+00:00",
        )

        self.assertEqual(
            "bronze/_control/landing_to_bronze/"
            "ingestion_date=2026-06-13/"
            "run_id=manual__2026-06-13T10_00_00_00_00/"
            "manifest.json",
            key,
        )

    def test_manifest_contains_auditable_totals(self):
        manifest = json.loads(
            build_manifest(
                run_id="manual__test",
                logical_date=datetime(2026, 6, 13, tzinfo=timezone.utc),
                database="ecommerce",
                results=[
                    {
                        "collection": "pedidos",
                        "source_files": 2,
                        "rows_written": 20,
                        "status": "written",
                    },
                    {
                        "collection": "clientes",
                        "source_files": 1,
                        "rows_written": 10,
                        "status": "written",
                    },
                ],
            )
        )

        self.assertEqual(30, manifest["total_rows_written"])
        self.assertEqual(3, manifest["total_source_files"])
        self.assertEqual("delta", manifest["target"]["format"])
        self.assertEqual(
            ["clientes", "pedidos"],
            [item["collection"] for item in manifest["collections"]],
        )

    def test_spark_packages_pin_compatible_versions(self):
        self.assertEqual(
            "io.delta:delta-spark_2.12:3.3.1,org.apache.hadoop:hadoop-aws:3.3.4",
            spark_packages(),
        )

    def test_spark_conf_supports_minio_and_https_s3(self):
        minio = build_spark_conf("http://minio:9000/")
        secure = build_spark_conf("https://s3.us-east-1.amazonaws.com")

        self.assertEqual("http://minio:9000", minio["spark.hadoop.fs.s3a.endpoint"])
        self.assertEqual(
            "false",
            minio["spark.hadoop.fs.s3a.connection.ssl.enabled"],
        )
        self.assertEqual(
            "true",
            secure["spark.hadoop.fs.s3a.connection.ssl.enabled"],
        )
        self.assertIn(
            "DeltaSparkSessionExtension",
            minio["spark.sql.extensions"],
        )

    def test_logical_date_is_normalized_to_utc(self):
        value = parse_logical_date("2026-06-13T09:30:00-03:00")

        self.assertEqual(
            datetime(2026, 6, 13, 12, 30, tzinfo=timezone.utc),
            value,
        )


if __name__ == "__main__":
    unittest.main()
