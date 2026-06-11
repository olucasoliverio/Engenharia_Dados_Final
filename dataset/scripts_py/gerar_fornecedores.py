import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")
Faker.seed(20)
random.seed(20)

TOTAL = 15_000
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "arquivos_csv", "fornecedores.csv")

ESTADOS = [
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
    "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
    "RS","RO","RR","SC","SP","SE","TO",
]

inicio = datetime(2023, 1, 1)
fim = datetime(2026, 6, 11)
delta_total = (fim - inicio).days


def data_aleatoria(inicio, delta):
    return inicio + timedelta(days=random.randint(0, delta))


registros = []
for i in range(1, TOTAL + 1):
    updated_at = data_aleatoria(inicio, delta_total)
    registros.append(
        {
            "id_fornecedor": i,
            "nome_fornecedor": fake.company(),
            "cnpj": fake.cnpj(),
            "email": fake.company_email(),
            "telefone": fake.phone_number(),
            "logradouro": fake.street_address(),
            "cidade": fake.city(),
            "estado": random.choice(ESTADOS),
            "cep": fake.postcode(),
            "updated_at": updated_at.isoformat(),
        }
    )

df = pd.DataFrame(registros)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"fornecedores.csv gerado: {len(df)} registros")
