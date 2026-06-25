#!/usr/bin/env bash
#
# reset_ambiente.sh — CANCELA TUDO E VOLTA DO ZERO.
#
# Derruba os containers, APAGA os volumes (MongoDB, MinIO e o banco de
# metadados do Airflow — todos os DAG runs somem) e limpa artefatos locais
# gerados. Por padrão, reconstrói o ambiente em seguida (chama setup_ambiente.sh).
#
# Uso:
#   ./scripts/reset_ambiente.sh              # apaga tudo e reconstrói do zero
#   ./scripts/reset_ambiente.sh --no-rebuild # só derruba e limpa (não reconstrói)
#   ./scripts/reset_ambiente.sh -y           # não pergunta confirmação
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

bold(){ printf "\033[1m%s\033[0m\n" "$*"; }
ok(){   printf "\033[32m✓ %s\033[0m\n" "$*"; }
info(){ printf "\033[36m→ %s\033[0m\n" "$*"; }

REBUILD=1
ASSUME_YES=0
for arg in "$@"; do
  case "$arg" in
    --no-rebuild) REBUILD=0 ;;
    -y|--yes)     ASSUME_YES=1 ;;
    -h|--help)    grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Opção desconhecida: $arg"; exit 2 ;;
  esac
done

bold "RESET — isto vai APAGAR:"
cat <<EOF
  • todos os containers do projeto
  • o volume do MongoDB         (dados das 10 coleções)
  • o volume do MinIO           (Data Lake: landing/bronze/silver/gold)
  • o volume do Postgres        (metadados do Airflow: histórico de DAG runs)
  • artefatos locais gerados    (airflow/logs, gold_export, CSVs, __pycache__)
EOF

if [ "$ASSUME_YES" -ne 1 ]; then
  printf "\033[33mTem certeza? digite 'sim' para continuar: \033[0m"
  read -r resposta
  [ "$resposta" = "sim" ] || { echo "Cancelado."; exit 0; }
fi

# ---- 1) derrubar containers + volumes ------------------------------------
bold "1/3 · Derrubando containers e removendo volumes"
docker compose down -v --remove-orphans
ok "Containers e volumes removidos"

# ---- 2) limpar artefatos locais ------------------------------------------
bold "2/3 · Limpando artefatos locais gerados"
rm -rf airflow/logs/* gold_export 2>/dev/null || true
rm -rf dataset/arquivos_csv 2>/dev/null || true   # CSVs são regerados pelo Faker
find . -type d -name "__pycache__" -prune -exec rm -rf {} + 2>/dev/null || true
find . -type d -name "*.egg-info"  -prune -exec rm -rf {} + 2>/dev/null || true
ok "Artefatos locais limpos"

# ---- 3) reconstruir (opcional) -------------------------------------------
if [ "$REBUILD" -eq 1 ]; then
  bold "3/3 · Reconstruindo do zero"
  exec "$SCRIPT_DIR/setup_ambiente.sh"
else
  bold "3/3 · Pulando reconstrução (--no-rebuild)"
  echo "Para subir de novo:  ./scripts/setup_ambiente.sh"
fi
