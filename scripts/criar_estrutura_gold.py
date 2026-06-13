"""Create and validate the Gold Delta structure in MinIO or Amazon S3."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    from scripts.lib import delta_structure
    from scripts.lib.object_storage import build_s3_client
except ModuleNotFoundError:
    from lib import delta_structure  # type: ignore[no-redef]
    from lib.object_storage import build_s3_client  # type: ignore[no-redef]


SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
DEFAULT_CONFIG_PATH = REPO_ROOT / "config" / "gold_structure.json"
GoldConfig = delta_structure.DeltaLayerConfig
build_structure_manifest = delta_structure.build_structure_manifest
delta_log_prefix = delta_structure.delta_log_prefix
structure_manifest_key = delta_structure.structure_manifest_key
table_marker_key = delta_structure.table_marker_key


def load_config(path: Path) -> GoldConfig:
    """Load and validate the versioned Gold contract."""
    return delta_structure.load_delta_config(
        path,
        expected_layer="gold",
        source_layer="silver",
        source_format="delta",
    )


def initialize_gold(
    s3_client: Any,
    config: GoldConfig,
    region: str = "us-east-1",
    enable_versioning: bool = True,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Idempotently create the Gold prefixes and structure manifest."""
    return delta_structure.initialize_delta_layer(
        s3_client,
        config,
        region=region,
        enable_versioning=enable_versioning,
        generated_at=generated_at,
    )


def validate_gold(s3_client: Any, config: GoldConfig) -> list[str]:
    """Return invalid required objects; an empty list means valid structure."""
    return delta_structure.validate_delta_layer(s3_client, config)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cria e valida a estrutura Gold Delta no MinIO ou S3."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Contrato JSON da Gold.",
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.getenv("S3_ENDPOINT_URL"),
        help="Endpoint do MinIO. Omita para usar Amazon S3.",
    )
    parser.add_argument(
        "--region",
        default=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        help="Regiao do bucket.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Apenas valida a estrutura sem criar objetos.",
    )
    parser.add_argument(
        "--no-versioning",
        action="store_true",
        help="Nao habilita versionamento no bucket.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        config = load_config(args.config)
        client = build_s3_client(args.endpoint_url, args.region)

        if not args.validate_only:
            result = initialize_gold(
                client,
                config,
                region=args.region,
                enable_versioning=not args.no_versioning,
            )
            print(
                "Estrutura criada/atualizada: "
                f"{result['table_count']} tabelas em "
                f"s3://{result['bucket']}/{config.layer}/"
            )
            print(
                f"Marcadores criados: {len(result['markers_created'])}; "
                "manifesto atualizado: "
                f"{'sim' if result['manifest_updated'] else 'nao'}"
            )

        invalid = validate_gold(client, config)
        if invalid:
            print("Estrutura Gold incompleta ou divergente:")
            for path in invalid:
                print(f"  - {path}")
            return 1

        print(f"Estrutura Gold valida: s3://{config.bucket}/{config.layer}/")
        return 0
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Erro de configuracao: {error}")
        return 2
    except Exception as error:
        print(f"Erro ao acessar o object storage: {error}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
