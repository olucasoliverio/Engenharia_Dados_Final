import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")
Faker.seed(70)
random.seed(70)

TOTAL = 15_000
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "arquivos_csv", "pagamentos.csv")

FORMAS = ["cartao_credito", "cartao_debito", "pix", "boleto"]
FORMAS_PESOS = [0.45, 0.20, 0.25, 0.10]
STATUS = ["aprovado", "pendente", "recusado", "estornado"]
STATUS_PESOS = [0.80, 0.10, 0.07, 0.03]

inicio = datetime(2023, 1, 1)
fim = datetime(2026, 6, 11)
delta_total = (fim - inicio).days


def data_aleatoria(inicio, delta):
    return inicio + timedelta(days=random.randint(0, delta))


registros = []
for i in range(1, TOTAL + 1):
    forma = random.choices(FORMAS, FORMAS_PESOS)[0]
    status = random.choices(STATUS, STATUS_PESOS)[0]
    parcelas = random.randint(1, 12) if forma == "cartao_credito" else 1
    data_pagamento = data_aleatoria(inicio, delta_total)
    updated_at = data_aleatoria(data_pagamento, (fim - data_pagamento).days or 1)
    registros.append(
        {
            "id_pagamento": i,
            "id_pedido": random.randint(1, TOTAL),
            "forma_pagamento": forma,
            "status_pagamento": status,
            "valor": round(random.uniform(20.0, 3000.0), 2),
            "data_pagamento": data_pagamento.isoformat(),
            "parcelas": parcelas,
            "updated_at": updated_at.isoformat(),
        }
    )

df = pd.DataFrame(registros)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"pagamentos.csv gerado: {len(df)} registros")
