# Validadores de schema — MongoDB

Definição machine-readable do modelo da origem (issue #6), no formato
[`$jsonSchema`](https://www.mongodb.com/docs/manual/core/schema-validation/)
do MongoDB. Há um arquivo por coleção do banco `ecommerce`.

| Arquivo | Coleção |
|---------|---------|
| `clientes.schema.json` | clientes |
| `categorias.schema.json` | categorias |
| `fornecedores.schema.json` | fornecedores |
| `produtos.schema.json` | produtos |
| `cupons.schema.json` | cupons |
| `pedidos.schema.json` | pedidos |
| `itens_pedido.schema.json` | itens_pedido |
| `pagamentos.schema.json` | pagamentos |
| `entregas.schema.json` | entregas |
| `avaliacoes.schema.json` | avaliacoes |

A descrição completa do modelo (campos, tipos, relacionamentos e estratégia
incremental) está em [`docs/modelo_mongodb.md`](../../docs/modelo_mongodb.md).

## Como aplicar

Os validadores são usados na criação das coleções (issues #8/#9). Exemplo com
o shell do MongoDB:

```js
const validator = JSON.parse(cat("dataset/schemas/clientes.schema.json"));
db.createCollection("clientes", { validator });
```

Ou em Python (`pymongo`), como será feito no script de carga:

```python
import json
from pymongo import MongoClient

with open("dataset/schemas/clientes.schema.json") as f:
    validator = json.load(f)

db = MongoClient(MONGO_URI)["ecommerce"]
db.create_collection("clientes", validator=validator)
```
