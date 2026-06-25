#!/usr/bin/env bash
#
# setup_ambiente.sh — sobe o ambiente completo e DEIXA O PIPELINE PRONTO PARA RODAR.
#
# Faz, em ordem:
#   1. sobe os containers (Mongo, MinIO, Airflow, Postgres)
#   2. espera tudo ficar "healthy"
#   3. cria/atualiza o venv e instala as dependências (uv se houver, senão pip)
#   4. popula o MongoDB com as 10 coleções (Faker)
#   5. cria a estrutura do Data Lake no MinIO (bucket + camadas)   <-- passo que faltava
#
# Ao final, as 4 DAGs do Airflow estão prontas para serem disparadas.
#
# Uso:  ./scripts/setup_ambiente.sh
set -euo pipefail

# ---- localização e .env ---------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

bold(){ printf "\033[1m%s\033[0m\n" "$*"; }
ok(){   printf "\033[32m✓ %s\033[0m\n" "$*"; }
info(){ printf "\033[36m→ %s\033[0m\n" "$*"; }
warn(){ printf "\033[33m! %s\033[0m\n" "$*"; }

if [ ! -f .env ]; then
  info "Criando .env a partir de .env.example"
  cp .env.example .env
fi
# carrega variáveis do .env (sem sobrescrever as já exportadas)
set -a; # shellcheck disable=SC1091
. ./.env 2>/dev/null || true
set +a

# defaults caso o .env não traga
S3_ENDPOINT_URL="${S3_ENDPOINT_URL:-http://localhost:9000}"
AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-${MINIO_ROOT_USER:-minioadmin}}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-${MINIO_ROOT_PASSWORD:-minioadmin}}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
export S3_ENDPOINT_URL AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION

# ---- 1) containers --------------------------------------------------------
bold "1/5 · Subindo containers (build pode demorar na 1ª vez)"
docker compose up -d --build \
  mongodb minio \
  airflow-apiserver airflow-scheduler airflow-dag-processor airflow-triggerer

# ---- 2) aguardar healthy --------------------------------------------------
bold "2/5 · Aguardando serviços ficarem saudáveis"
wait_healthy(){
  local svc="$1" tries="${2:-60}" cid
  for _ in $(seq 1 "$tries"); do
    cid="$(docker compose ps -q "$svc" 2>/dev/null || true)"
    if [ -n "$cid" ]; then
      local st
      st="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || echo "?")"
      [ "$st" = "healthy" ] || [ "$st" = "running" ] && { ok "$svc: $st"; return 0; }
    fi
    sleep 3
  done
  warn "$svc não ficou saudável a tempo — siga, mas confira 'docker compose ps'."
}
wait_healthy mongodb
wait_healthy minio
wait_healthy airflow-apiserver 80

# ---- 3) venv + dependências ----------------------------------------------
bold "3/5 · Preparando venv e dependências locais"
if command -v uv >/dev/null 2>&1; then
  PYRUN(){ uv run --python .venv "$@"; }
  [ -d .venv ] || uv venv
  uv pip install --python .venv -q ".[dataset,infra]"
  ok "Dependências instaladas via uv (.[dataset,infra])"
else
  [ -d .venv ] || python3 -m venv .venv
  # shellcheck disable=SC1091
  . .venv/bin/activate
  pip install -q ".[dataset,infra]"
  PYRUN(){ .venv/bin/python "$@"; }
  ok "Dependências instaladas via pip (.[dataset,infra])"
fi

# ---- 4) popular o MongoDB -------------------------------------------------
bold "4/5 · Populando o MongoDB (10 coleções)"
PYRUN dataset/scripts_py/carregar_mongo.py
ok "MongoDB populado"

# ---- 5) estrutura do Data Lake no MinIO ----------------------------------
bold "5/5 · Criando a estrutura do Data Lake no MinIO"
for layer in landing bronze silver gold; do
  info "camada $layer"
  PYRUN "scripts/criar_estrutura_${layer}.py"
done
ok "Estrutura do Data Lake criada (bucket 'datalake': landing/bronze/silver/gold)"

# ---- pronto ---------------------------------------------------------------
echo
bold "Ambiente pronto! 🎉"
cat <<EOF

Próximo passo — disparar as DAGs, NESTA ordem, em http://localhost:8080 (airflow/airflow):
    mongodb_to_landing → landing_to_bronze → bronze_to_silver → silver_to_gold

UIs:  Airflow http://localhost:8080   ·   MinIO  http://localhost:9001 (minioadmin/minioadmin)
Para zerar tudo e recomeçar:  ./scripts/reset_ambiente.sh
EOF
