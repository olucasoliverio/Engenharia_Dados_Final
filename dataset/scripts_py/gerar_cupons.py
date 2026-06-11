import os
import random
import string
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")
Faker.seed(40)
random.seed(40)

TOTAL = 15_000
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "arquivos_csv", "cupons.csv")

DESCONTOS = [5, 10, 15, 20, 25, 30, 40, 50]
VALORES_MINIMOS = [50, 100, 150, 200, 300, 500]

inicio = datetime(2023, 1, 1)
fim = datetime(2027, 12, 31)
delta_total = (fim - inicio).days

criacao_inicio = datetime(2023, 1, 1)
criacao_fim = datetime(2026, 6, 11)
delta_criacao = (criacao_fim - criacao_inicio).days


def data_aleatoria(inicio, delta):
    return inicio + timedelta(days=random.randint(0, delta))


def gerar_codigo():
    prefixos = ["PROMO", "DESC", "OFF", "SALE", "SAVE", "MEGA", "SUPER", "TOP"]
    prefixo = random.choice(prefixos)
    sufixo = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"{prefixo}{sufixo}"


registros = []
codigos_usados = set()
for i in range(1, TOTAL + 1):
    codigo = gerar_codigo()
    while codigo in codigos_usados:
        codigo = gerar_codigo()
    codigos_usados.add(codigo)

    data_validade = data_aleatoria(inicio, delta_total)
    updated_at = data_aleatoria(criacao_inicio, delta_criacao)
    ativo = data_validade >= datetime(2026, 6, 11) and random.random() > 0.3

    registros.append(
        {
            "id_cupom": i,
            "codigo": codigo,
            "desconto_percentual": random.choice(DESCONTOS),
            "valor_minimo": random.choice(VALORES_MINIMOS),
            "data_validade": data_validade.date().isoformat(),
            "ativo": ativo,
            "updated_at": updated_at.isoformat(),
        }
    )

df = pd.DataFrame(registros)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"cupons.csv gerado: {len(df)} registros")
