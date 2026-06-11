# Engenharia de Dados — Projeto Final

Pipeline de dados de um e-commerce fictício (origem MongoDB → Data Lake medalhão
→ dashboard). Documentação completa publicada via MkDocs.

## Integrantes

- Guilherme Madalena
- Gustavo Felisbino
- Lucas Gaspar
- Lucas Oliverio
- Luiz Barros
- Tiago Mazzuco

## Ambiente MongoDB local (origem)

A origem dos dados é um MongoDB rodando em Docker. O modelo das coleções está
documentado em [`docs/modelo_mongodb.md`](docs/modelo_mongodb.md).

### Pré-requisitos

- Docker + Docker Compose

### Subir o banco

```bash
# 1. Crie seu .env a partir do exemplo (não é commitado)
cp .env.example .env

# 2. Suba o container
docker compose up -d

# 3. Verifique a saúde do container (aguarde STATUS = healthy)
docker compose ps
```

O serviço sobe em `localhost:27017` com o banco `ecommerce`. As credenciais e a
string de conexão ficam no `.env` (veja `.env.example`).

### Parar / limpar

```bash
docker compose down        # para o container (mantém os dados no volume)
docker compose down -v     # para e APAGA os dados (remove o volume)
```

> A população das coleções com os dados simulados é feita por um script
> separado (issue #9).
