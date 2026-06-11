import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")
Faker.seed(50)
random.seed(50)

TOTAL = 15_000
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "arquivos_csv", "pedidos.csv")

STATUS_PEDIDO = ["pendente", "processando", "enviado", "entregue", "cancelado"]
STATUS_PESOS = [0.05, 0.10, 0.15, 0.55, 0.15]

inicio = datetime(2023, 1, 1)
fim = datetime(2026, 6, 11)
delta_total = (fim - inicio).days


def data_aleatoria(inicio, delta):
    return inicio + timedelta(days=random.randint(0, delta))


registros = []
for i in range(1, TOTAL + 1):
    data_pedido = data_aleatoria(inicio, delta_total)
    updated_at = data_aleatoria(data_pedido, (fim - data_pedido).days or 1)
    tem_cupom = random.random() < 0.25
    registros.append(
        {
            "id_pedido": i,
            "id_cliente": random.randint(1, TOTAL),
            "data_pedido": data_pedido.isoformat(),
            "status": random.choices(STATUS_PEDIDO, STATUS_PESOS)[0],
            "valor_total": round(random.uniform(20.0, 3000.0), 2),
            "id_cupom": random.randint(1, TOTAL) if tem_cupom else None,
            "updated_at": updated_at.isoformat(),
        }
    )

df = pd.DataFrame(registros)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"pedidos.csv gerado: {len(df)} registros")
