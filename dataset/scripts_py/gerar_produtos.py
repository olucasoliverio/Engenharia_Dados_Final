import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")
Faker.seed(30)
random.seed(30)

TOTAL = 15_000
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "arquivos_csv", "produtos.csv")

MARCAS = [
    "Samsung", "Apple", "Sony", "LG", "Philips", "Braun", "Tramontina",
    "Nike", "Adidas", "Puma", "Havaianas", "Localfrio", "Electrolux",
    "Multilaser", "Positivo", "Intelbras", "Mondial", "Arno", "Britânia",
    "Oster", "Panasonic", "Motorola", "Xiaomi", "Dell", "HP", "Lenovo",
]

ADJETIVOS = ["Premium", "Pro", "Plus", "Max", "Lite", "Ultra", "Smart", "Slim", "Turbo", "Super"]

inicio = datetime(2023, 1, 1)
fim = datetime(2026, 6, 11)
delta_total = (fim - inicio).days


def data_aleatoria(inicio, delta):
    return inicio + timedelta(days=random.randint(0, delta))


registros = []
for i in range(1, TOTAL + 1):
    marca = random.choice(MARCAS)
    adj = random.choice(ADJETIVOS)
    nome = f"{marca} {adj} {fake.word().capitalize()} {random.randint(100, 9999)}"
    preco = round(random.uniform(9.90, 4999.90), 2)
    updated_at = data_aleatoria(inicio, delta_total)
    registros.append(
        {
            "id_produto": i,
            "nome_produto": nome,
            "descricao": fake.sentence(nb_words=10),
            "preco": preco,
            "estoque": random.randint(0, 500),
            "id_categoria": random.randint(1, TOTAL),
            "id_fornecedor": random.randint(1, TOTAL),
            "marca": marca,
            "peso_kg": round(random.uniform(0.1, 30.0), 2),
            "updated_at": updated_at.isoformat(),
        }
    )

df = pd.DataFrame(registros)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"produtos.csv gerado: {len(df)} registros")
