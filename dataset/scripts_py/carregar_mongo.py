"""
Carga dos dados simulados (CSV) para o MongoDB de origem (issue #9).

Le os 10 CSVs de `dataset/arquivos_csv/`, converte os tipos (datas viram
ISODate, numeros viram int/float, `ativo` vira bool, campos vazios viram null)
e insere em cada colecao do banco `ecommerce`. Aplica os validadores
`$jsonSchema` definidos em `mongodb/schemas/` (issue #6) e cria indices de
chave primaria, chaves estrangeiras e `updated_at` (carga incremental).

Conexao: le `MONGO_URI` e `MONGO_DB` do ambiente ou do arquivo `.env` na raiz
do repositorio. Default: MongoDB local do docker-compose.

Uso:
    python carregar_mongo.py                  # carrega todas as colecoes
    python carregar_mongo.py --only pedidos   # apenas uma colecao
    python carregar_mongo.py --no-validator   # nao aplica $jsonSchema
"""

from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError

# --- Caminhos relativos ao repositorio ---------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent
CSV_DIR = REPO_ROOT / "dataset" / "arquivos_csv"
SCHEMA_DIR = REPO_ROOT / "mongodb" / "schemas"
ENV_FILE = REPO_ROOT / ".env"

DEFAULT_URI = "mongodb://admin:admin123@localhost:27017/?authSource=admin"
DEFAULT_DB = "ecommerce"
BATCH_SIZE = 5_000

# --- Definicao de tipos por colecao ------------------------------------------
# Para cada colecao: arquivo CSV, coluna de chave primaria, colunas inteiras,
# decimais, booleanas, de data e chaves estrangeiras (para indices).
COLECOES = {
    "clientes": {
        "csv": "clientes.csv",
        "pk": "id_cliente",
        "int": ["id_cliente"],
        "float": [],
        "bool": [],
        "date": ["data_nascimento", "data_cadastro", "updated_at"],
        "fk": [],
    },
    "categorias": {
        "csv": "categorias.csv",
        "pk": "id_categoria",
        "int": ["id_categoria"],
        "float": [],
        "bool": [],
        "date": ["updated_at"],
        "fk": [],
    },
    "fornecedores": {
        "csv": "fornecedores.csv",
        "pk": "id_fornecedor",
        "int": ["id_fornecedor"],
        "float": [],
        "bool": [],
        "date": ["updated_at"],
        "fk": [],
    },
    "produtos": {
        "csv": "produtos.csv",
        "pk": "id_produto",
        "int": ["id_produto", "estoque", "id_categoria", "id_fornecedor"],
        "float": ["preco", "peso_kg"],
        "bool": [],
        "date": ["updated_at"],
        "fk": ["id_categoria", "id_fornecedor"],
    },
    "cupons": {
        "csv": "cupons.csv",
        "pk": "id_cupom",
        "int": ["id_cupom", "desconto_percentual", "valor_minimo"],
        "float": [],
        "bool": ["ativo"],
        "date": ["data_validade", "updated_at"],
        "fk": [],
    },
    "pedidos": {
        "csv": "pedidos.csv",
        "pk": "id_pedido",
        "int": ["id_pedido", "id_cliente", "id_cupom"],
        "float": ["valor_total"],
        "bool": [],
        "date": ["data_pedido", "updated_at"],
        "fk": ["id_cliente", "id_cupom"],
    },
    "itens_pedido": {
        "csv": "itens_pedido.csv",
        "pk": "id_item",
        "int": ["id_item", "id_pedido", "id_produto", "quantidade"],
        "float": ["valor_unitario", "desconto_percentual", "subtotal"],
        "bool": [],
        "date": ["updated_at"],
        "fk": ["id_pedido", "id_produto"],
    },
    "pagamentos": {
        "csv": "pagamentos.csv",
        "pk": "id_pagamento",
        "int": ["id_pagamento", "id_pedido", "parcelas"],
        "float": ["valor"],
        "bool": [],
        "date": ["data_pagamento", "updated_at"],
        "fk": ["id_pedido"],
    },
    "entregas": {
        "csv": "entregas.csv",
        "pk": "id_entrega",
        "int": ["id_entrega", "id_pedido"],
        "float": [],
        "bool": [],
        "date": ["data_envio", "data_entrega_prevista", "data_entrega_real", "updated_at"],
        "fk": ["id_pedido"],
    },
    "avaliacoes": {
        "csv": "avaliacoes.csv",
        "pk": "id_avaliacao",
        "int": ["id_avaliacao", "id_pedido", "id_cliente", "id_produto", "nota"],
        "float": [],
        "bool": [],
        "date": ["data_avaliacao", "updated_at"],
        "fk": ["id_pedido", "id_cliente", "id_produto"],
    },
}


def carregar_env(path: Path) -> dict[str, str]:
    """Le um .env simples (KEY=VALUE) ignorando comentarios e linhas vazias."""
    valores: dict[str, str] = {}
    if not path.exists():
        return valores
    for linha in path.read_text(encoding="utf-8").splitlines():
        linha = linha.strip()
        if not linha or linha.startswith("#") or "=" not in linha:
            continue
        chave, _, valor = linha.partition("=")
        valores[chave.strip()] = valor.strip().strip('"').strip("'")
    return valores


def resolver_conexao(args) -> tuple[str, str]:
    """Resolve URI e nome do banco: CLI > ambiente > .env > default."""
    import os

    env = carregar_env(ENV_FILE)
    uri = args.uri or os.environ.get("MONGO_URI") or env.get("MONGO_URI") or DEFAULT_URI
    db = args.db or os.environ.get("MONGO_DB") or env.get("MONGO_DB") or DEFAULT_DB
    return uri, db


def converter_valor(valor: str, coluna: str, tipos: dict) -> object:
    """Converte uma celula do CSV para o tipo BSON adequado."""
    if valor is None or valor == "":
        return None
    if coluna in tipos["int"]:
        return int(float(valor)) if valor else None
    if coluna in tipos["float"]:
        return float(valor)
    if coluna in tipos["bool"]:
        return str(valor).strip().lower() in ("true", "1", "sim")
    if coluna in tipos["date"]:
        return datetime.fromisoformat(valor)
    return valor


def ler_documentos(csv_path: Path, tipos: dict):
    """Gera documentos convertidos a partir do CSV."""
    with csv_path.open(encoding="utf-8", newline="") as f:
        for linha in csv.DictReader(f):
            yield {col: converter_valor(val, col, tipos) for col, val in linha.items()}


def carregar_validador(nome: str) -> dict | None:
    schema_path = SCHEMA_DIR / f"{nome}.schema.json"
    if not schema_path.exists():
        return None
    return json.loads(schema_path.read_text(encoding="utf-8"))


def carregar_colecao(db, nome: str, tipos: dict, usar_validador: bool) -> int:
    csv_path = CSV_DIR / tipos["csv"]
    if not csv_path.exists():
        print(f"  ! CSV nao encontrado: {csv_path} — pulando")
        return 0

    # Recria a colecao (idempotente).
    db.drop_collection(nome)
    validador = carregar_validador(nome) if usar_validador else None
    if validador:
        db.create_collection(nome, validator=validador)
    else:
        db.create_collection(nome)
    colecao = db[nome]

    total = 0
    lote: list[dict] = []
    for doc in ler_documentos(csv_path, tipos):
        lote.append(doc)
        if len(lote) >= BATCH_SIZE:
            colecao.insert_many(lote, ordered=False)
            total += len(lote)
            lote = []
    if lote:
        colecao.insert_many(lote, ordered=False)
        total += len(lote)

    # Indices: chave primaria (unica), chaves estrangeiras e updated_at.
    colecao.create_index([(tipos["pk"], ASCENDING)], unique=True)
    for fk in tipos["fk"]:
        colecao.create_index([(fk, ASCENDING)])
    colecao.create_index([("updated_at", ASCENDING)])

    print(f"  ok {nome}: {total:,} documentos" + (" (com validador)" if validador else ""))
    return total


def garantir_csvs(alvos: dict) -> bool:
    """Gera os CSVs ausentes chamando gerar_dados.py. Retorna False se falhar."""
    faltando = [
        nome for nome, tipos in alvos.items() if not (CSV_DIR / tipos["csv"]).exists()
    ]
    if not faltando:
        return True
    print(f"CSV(s) ausente(s): {', '.join(faltando)} — gerando o dataset...")
    gerador = SCRIPT_DIR / "gerar_dados.py"
    try:
        subprocess.run([sys.executable, str(gerador)], check=True)
    except subprocess.CalledProcessError as e:
        print(f"ERRO ao gerar os CSVs ({e}).")
        print("Instale as dependencias de geracao: pip install -r "
              "dataset/scripts_py/requirements.txt")
        return False
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Carrega os CSVs no MongoDB de origem.")
    parser.add_argument("--uri", help="String de conexao do MongoDB (sobrescreve .env)")
    parser.add_argument("--db", help="Nome do banco (default: ecommerce)")
    parser.add_argument("--only", help="Carrega apenas a colecao informada")
    parser.add_argument("--no-validator", action="store_true", help="Nao aplica os $jsonSchema")
    parser.add_argument(
        "--no-gerar",
        action="store_true",
        help="Nao gera os CSVs automaticamente quando estiverem ausentes",
    )
    args = parser.parse_args()

    uri, db_nome = resolver_conexao(args)
    alvos = {args.only: COLECOES[args.only]} if args.only else COLECOES
    if args.only and args.only not in COLECOES:
        print(f"Colecao invalida: {args.only}. Opcoes: {', '.join(COLECOES)}")
        return 2

    if not args.no_gerar and not garantir_csvs(alvos):
        return 1

    # Esconde a senha ao imprimir a URI.
    uri_segura = uri.split("@")[-1] if "@" in uri else uri
    print(f"Conectando em ...@{uri_segura} | banco: {db_nome}")

    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=8000)
        client.admin.command("ping")
    except PyMongoError as e:
        print(f"ERRO ao conectar no MongoDB: {e}")
        print("Verifique se o container esta de pe (docker compose up -d) e o MONGO_URI no .env.")
        return 1

    db = client[db_nome]
    inicio = time.perf_counter()
    print(f"Carregando {len(alvos)} colecao(oes)...")
    total_geral = 0
    for nome, tipos in alvos.items():
        total_geral += carregar_colecao(db, nome, tipos, usar_validador=not args.no_validator)
    dur = time.perf_counter() - inicio

    print(f"\nConcluido: {total_geral:,} documentos em {len(alvos)} colecao(oes) ({dur:.1f}s).")
    print("Colecoes no banco:", ", ".join(sorted(db.list_collection_names())))
    client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
