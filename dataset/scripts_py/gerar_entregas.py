import os
import random
import string
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")
Faker.seed(80)
random.seed(80)

TOTAL = 15_000
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "arquivos_csv", "entregas.csv")

STATUS = ["pendente", "em_transito", "entregue", "devolvido"]
STATUS_PESOS = [0.08, 0.15, 0.72, 0.05]
TRANSPORTADORAS = ["Correios", "JadLog", "Total Express", "Loggi", "Azul Cargo", "Latam Cargo", "DHL", "FedEx"]

inicio = datetime(2023, 1, 1)
fim = datetime(2026, 6, 11)
delta_total = (fim - inicio).days


def data_aleatoria(inicio, delta):
    return inicio + timedelta(days=random.randint(0, delta))


def gerar_rastreio():
    letras = "".join(random.choices(string.ascii_uppercase, k=2))
    numeros = "".join(random.choices(string.digits, k=9))
    return f"{letras}{numeros}BR"


registros = []
for i in range(1, TOTAL + 1):
    status = random.choices(STATUS, STATUS_PESOS)[0]
    data_envio = data_aleatoria(inicio, delta_total)
    prazo_dias = random.randint(3, 20)
    data_prevista = data_envio + timedelta(days=prazo_dias)

    if status == "entregue":
        atraso = random.randint(-2, 5)
        data_real = data_prevista + timedelta(days=atraso)
    elif status == "devolvido":
        data_real = data_prevista + timedelta(days=random.randint(5, 15))
    else:
        data_real = None

    updated_at = data_aleatoria(data_envio, (fim - data_envio).days or 1)
    registros.append(
        {
            "id_entrega": i,
            "id_pedido": random.randint(1, TOTAL),
            "status_entrega": status,
            "data_envio": data_envio.isoformat(),
            "data_entrega_prevista": data_prevista.date().isoformat(),
            "data_entrega_real": data_real.date().isoformat() if data_real else None,
            "transportadora": random.choice(TRANSPORTADORAS),
            "codigo_rastreio": gerar_rastreio(),
            "updated_at": updated_at.isoformat(),
        }
    )

df = pd.DataFrame(registros)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"entregas.csv gerado: {len(df)} registros")
