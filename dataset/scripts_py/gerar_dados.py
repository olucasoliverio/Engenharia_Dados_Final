"""Gera todos os CSVs do dataset executando os 10 scripts ``gerar_*.py``.

Os scripts usam sementes fixas, então a saída é determinística. Requer
``faker`` e ``pandas`` (veja requirements.txt). Uso:

    python gerar_dados.py
"""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = SCRIPT_DIR.parent / "arquivos_csv"


def geradores() -> list[Path]:
    return sorted(
        s for s in SCRIPT_DIR.glob("gerar_*.py") if s.name != "gerar_dados.py"
    )


def main() -> int:
    scripts = geradores()
    if not scripts:
        print("Nenhum script gerar_*.py encontrado.")
        return 1
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for script in scripts:
        print(f"-> {script.name}")
        runpy.run_path(str(script), run_name="__main__")
    print(f"\nOK: {len(scripts)} arquivos gerados em {OUTPUT_DIR}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
