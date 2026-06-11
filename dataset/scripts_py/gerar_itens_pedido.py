import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")
Faker.seed(60)
random.seed(60)

TOTAL = 15_000
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "arquivos_csv", "itens_pedido.csv")

inicio = datetime(2023, 1, 1)
fim = datetime(2026, 6, 11)
delta_total = (fim - inicio).days


def data_aleatoria(inicio, delta):
    return inicio + timedelta(days=random.randint(0, delta))


registros = []
for i in range(1, TOTAL + 1):
    quantidade = random.randint(1, 10)
    valor_unitario = round(random.uniform(9.90, 2999.90), 2)
    desconto = round(random.uniform(0, 0.30), 2)
    subtotal = round(quantidade * valor_unitario * (1 - desconto), 2)
    updated_at = data_aleatoria(inicio, delta_total)
    registros.append(
        {
            "id_item": i,
            "id_pedido": random.randint(1, TOTAL),
            "id_produto": random.randint(1, TOTAL),
            "quantidade": quantidade,
            "valor_unitario": valor_unitario,
            "desconto_percentual": round(desconto * 100, 1),
            "subtotal": subtotal,
            "updated_at": updated_at.isoformat(),
        }
    )

df = pd.DataFrame(registros)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"itens_pedido.csv gerado: {len(df)} registros")
