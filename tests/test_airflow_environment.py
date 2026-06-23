from __future__ import annotations

import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


class AirflowEnvironmentConfigTest(unittest.TestCase):
    def test_compose_declares_airflow_stack(self):
        compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")

        for service_name in (
            "airflow-postgres",
            "airflow-init",
            "airflow-apiserver",
            "airflow-scheduler",
            "airflow-dag-processor",
            "airflow-triggerer",
        ):
            self.assertIn(f"  {service_name}:", compose)

        self.assertIn("AIRFLOW__CORE__EXECUTOR: LocalExecutor", compose)
        self.assertIn("./dags:/opt/airflow/dags", compose)
        self.assertIn("postgresql+psycopg2://airflow:airflow@airflow-postgres/airflow", compose)

    def test_airflow_requirements_include_dag_providers(self):
        import tomllib

        pyproject = tomllib.loads(
            (REPO_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        )
        airflow_deps = pyproject["project"]["optional-dependencies"]["airflow"]
        providers = {dep.split("==", 1)[0].split(">", 1)[0].strip() for dep in airflow_deps}

        self.assertIn("apache-airflow-providers-amazon", providers)
        self.assertIn("apache-airflow-providers-mongo", providers)

    def test_env_example_exposes_airflow_connections(self):
        env_example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")

        self.assertIn("MONGO_CONN_ID=mongodb_atlas", env_example)
        self.assertIn("S3_CONN_ID=minio_s3", env_example)
        self.assertIn("AIRFLOW_CONN_MONGODB_ATLAS=", env_example)
        self.assertIn("AIRFLOW_CONN_MINIO_S3=", env_example)


if __name__ == "__main__":
    unittest.main()
