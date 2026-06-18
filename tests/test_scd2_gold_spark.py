"""Testes de integração do SCD Tipo 2 da camada Gold (PySpark + Delta).

Rodam apenas quando o PySpark/Delta estão disponíveis (ex.: venv Python 3.13).
Na suíte padrão (Python sem PySpark) a classe é pulada automaticamente.
"""

from __future__ import annotations

import shutil
import tempfile
import unittest
from datetime import date, datetime

try:  # PySpark só existe no ambiente de execução do Spark
    import pyspark  # noqa: F401
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    HAS_SPARK = True
except Exception:  # pragma: no cover - ambiente sem Spark
    HAS_SPARK = False

if HAS_SPARK:
    from spark_jobs.silver_to_gold import (
        _add_scd2_attributes_hash,
        _attach_dimension_sk,
        _sync_dimension_scd2,
    )
    from dags.lib.silver_gold import (
        SCD2_BEGINNING_OF_TIME,
        SCD2_END_OF_TIME,
    )


@unittest.skipUnless(HAS_SPARK, "PySpark/Delta indisponíveis neste ambiente")
class Scd2GoldIntegrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        builder = (
            SparkSession.builder.appName("scd2-test")
            .master("local[1]")
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
        )
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="scd2_")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _dim_source(self, rows):
        # rows: (cliente_key, nome, cidade)
        data = [
            (key, nome, "F", date(1990, 1, 1), cidade, "SP", date(2023, 1, 1))
            for key, nome, cidade in rows
        ]
        df = self.spark.createDataFrame(
            data,
            schema=(
                "cliente_key int, nome string, genero string, "
                "data_nascimento date, cidade string, estado string, "
                "data_cadastro date"
            ),
        )
        return _add_scd2_attributes_hash(df, natural_key="cliente_key")

    def test_initial_load_marks_every_row_current_from_beginning_of_time(self):
        path = f"{self.tmp}/dim_cliente"
        metrics = _sync_dimension_scd2(
            self.spark,
            self._dim_source([(1, "Ana", "Sao Paulo"), (2, "Bob", "Rio")]),
            output_uri=path,
            natural_key="cliente_key",
            surrogate_key="cliente_sk",
            run_id="run-1",
        )

        self.assertEqual(2, metrics["inserted"])
        self.assertEqual(2, metrics["versions_inserted"])
        self.assertEqual(0, metrics["versions_expired"])

        dim = self.spark.read.format("delta").load(path)
        self.assertEqual(2, dim.count())
        self.assertEqual(2, dim.filter(F.col("dw_is_current")).count())
        # Surrogate única por versão.
        self.assertEqual(2, dim.select("cliente_sk").distinct().count())
        # Primeira versão vigente "desde sempre".
        begin_col = F.date_format(F.col("dw_valid_from"), "yyyy-MM-dd HH:mm:ss")
        self.assertEqual(
            2, dim.filter(begin_col == F.lit(SCD2_BEGINNING_OF_TIME)).count()
        )

    def test_changed_attribute_expires_old_version_and_inserts_new(self):
        path = f"{self.tmp}/dim_cliente"
        common = dict(
            output_uri=path,
            natural_key="cliente_key",
            surrogate_key="cliente_sk",
        )
        _sync_dimension_scd2(
            self.spark,
            self._dim_source([(1, "Ana", "Sao Paulo"), (2, "Bob", "Rio")]),
            run_id="run-1",
            **common,
        )
        # Cliente 1 muda de cidade; cliente 2 inalterado.
        metrics = _sync_dimension_scd2(
            self.spark,
            self._dim_source([(1, "Ana", "Belo Horizonte"), (2, "Bob", "Rio")]),
            run_id="run-2",
            **common,
        )

        self.assertEqual(0, metrics["inserted"])
        self.assertEqual(1, metrics["updated"])
        self.assertEqual(1, metrics["unchanged"])
        self.assertEqual(1, metrics["versions_expired"])
        self.assertEqual(1, metrics["versions_inserted"])

        dim = self.spark.read.format("delta").load(path)
        self.assertEqual(3, dim.count())  # 2 originais + 1 nova versão

        cliente1 = dim.filter(F.col("cliente_key") == 1)
        self.assertEqual(2, cliente1.count())
        atual = cliente1.filter(F.col("dw_is_current")).collect()
        self.assertEqual(1, len(atual))
        self.assertEqual("Belo Horizonte", atual[0]["cidade"])

        expirada = cliente1.filter(~F.col("dw_is_current")).collect()
        self.assertEqual(1, len(expirada))
        self.assertEqual("Sao Paulo", expirada[0]["cidade"])
        end = datetime.strptime(SCD2_END_OF_TIME, "%Y-%m-%d %H:%M:%S")
        self.assertNotEqual(end, expirada[0]["dw_valid_to"])
        # A versão expirada termina onde a nova começa.
        self.assertEqual(expirada[0]["dw_valid_to"], atual[0]["dw_valid_from"])

    def test_point_in_time_join_picks_version_valid_at_event_date(self):
        # Dimensão versionada montada manualmente (2 versões do cliente 1).
        dim = self.spark.createDataFrame(
            [
                (1, "SK_A", datetime(2020, 1, 1), datetime(2023, 1, 1), False),
                (1, "SK_B", datetime(2023, 1, 1), datetime(9999, 12, 31), True),
            ],
            schema=(
                "cliente_key int, cliente_sk string, "
                "dw_valid_from timestamp, dw_valid_to timestamp, "
                "dw_is_current boolean"
            ),
        )
        fato = self.spark.createDataFrame(
            [
                (100, 1, date(2022, 6, 1)),  # antes da mudança -> SK_A
                (200, 1, date(2024, 6, 1)),  # depois da mudança -> SK_B
            ],
            schema="venda_key int, cliente_key int, data_pedido date",
        )

        resultado = _attach_dimension_sk(
            fato,
            dim,
            natural_key="cliente_key",
            surrogate_key="cliente_sk",
            event_date_col="data_pedido",
        )
        sks = {
            row["venda_key"]: row["cliente_sk"]
            for row in resultado.collect()
        }
        self.assertEqual("SK_A", sks[100])
        self.assertEqual("SK_B", sks[200])


if __name__ == "__main__":
    unittest.main()
