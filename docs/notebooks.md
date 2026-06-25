---
tags:
  - notebooks
  - jupyter
  - documentação
---

# Notebooks (Jupyter)

Além desta documentação MkDocs, o projeto traz uma **documentação interativa** em
**10 notebooks Jupyter** na pasta [`notebooks/`](https://github.com/olucasoliverio/Engenharia_Dados_Final/tree/main/notebooks).
São uma narrativa navegável — útil para estudar o projeto passo a passo e para a
apresentação — que complementa as páginas deste site.

!!! abstract "Em resumo"
    - **10 notebooks** numerados (`00` → `09`), do índice à execução do pipeline.
    - Abrem no **Jupyter Lab / Notebook**; dependências no grupo `[notebooks]` do `pyproject.toml`.
    - Ponto de partida: `00_indice_documentacao.ipynb`.

## Conteúdo

| Notebook | Conteúdo |
|---|---|
| `00_indice_documentacao` | Índice e ponto de partida da documentação |
| `01_visao_geral_projeto` | Visão geral do pipeline e do domínio |
| `02_estrutura_repositorio` | Organização das pastas e arquivos |
| `03_arquitetura_detalhada` | Arquitetura medalhão em detalhe |
| `04_processos_negocio` | Processos de negócio do e-commerce |
| `05_fluxo_dados` | Fluxo de dados de ponta a ponta |
| `06_banco_dados` | Camada de origem (MongoDB) |
| `07_interfaces_apis` | Interfaces e APIs do projeto |
| `08_infraestrutura` | Infraestrutura (Docker, MinIO, Airflow) |
| `09_execucao_pipeline` | Guia prático de execução |

## Como abrir

```bash
uv venv && source .venv/bin/activate
uv pip install ".[notebooks]"     # instala notebook + jupyterlab

jupyter lab                    # abre no navegador (ou: jupyter notebook)
```

Depois abra `notebooks/00_indice_documentacao.ipynb` e siga a numeração.

!!! tip "MkDocs × Notebooks"
    As duas documentações cobrem o mesmo projeto com formatos diferentes: o
    **MkDocs** (este site) é a referência publicada e pesquisável; os
    **notebooks** são a versão interativa, ideal para rodar e explorar localmente.

## Referências

- [Project Jupyter](https://jupyter.org/)
- [JupyterLab](https://jupyterlab.readthedocs.io/)
- Página completa de [referências](referencias.md)
