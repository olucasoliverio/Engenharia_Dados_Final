import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")
Faker.seed(42)
random.seed(42)

TOTAL = 15_000
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "arquivos_csv", "clientes.csv")

GENEROS = ["M", "F", "Outro"]
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
    data_cadastro = data_aleatoria(inicio, delta_total)
    updated_at = data_aleatoria(data_cadastro, (fim - data_cadastro).days)
    registros.append(
        {
            "id_cliente": i,
            "nome": fake.name(),
            "email": fake.email(),
            "cpf": fake.cpf(),
            "telefone": fake.phone_number(),
            "data_nascimento": fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
            "genero": random.choice(GENEROS),
            "logradouro": fake.street_address(),
            "cidade": fake.city(),
            "estado": random.choice(ESTADOS),
            "cep": fake.postcode(),
            "data_cadastro": data_cadastro.isoformat(),
            "updated_at": updated_at.isoformat(),
        }
    )

df = pd.DataFrame(registros)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"clientes.csv gerado: {len(df)} registros")
