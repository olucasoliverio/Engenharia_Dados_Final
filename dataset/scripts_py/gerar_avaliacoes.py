import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")
Faker.seed(90)
random.seed(90)

TOTAL = 15_000
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "arquivos_csv", "avaliacoes.csv")

COMENTARIOS_POS = [
    "Produto excelente, superou minhas expectativas!",
    "Entrega rápida e produto de ótima qualidade.",
    "Muito satisfeito com a compra, recomendo!",
    "Ótimo custo-benefício, chegou antes do prazo.",
    "Produto conforme descrito, embalagem perfeita.",
    "Atendimento incrível e produto top!",
    "Comprei e já quero comprar de novo. Perfeito!",
]

COMENTARIOS_NEU = [
    "Produto ok, dentro do esperado.",
    "Chegou no prazo, produto razoável.",
    "Nada de mais, mas cumpre o que promete.",
    "Poderia ter mais opções, mas está bom.",
    "Qualidade mediana pelo preço cobrado.",
]

COMENTARIOS_NEG = [
    "Produto com defeito, decepcionante.",
    "Demorou muito para chegar e veio errado.",
    "Qualidade muito abaixo do esperado.",
    "Não recomendo, produto diferente do anunciado.",
    "Péssima experiência, não voltarei a comprar.",
]

inicio = datetime(2023, 1, 1)
fim = datetime(2026, 6, 11)
delta_total = (fim - inicio).days


def data_aleatoria(inicio, delta):
    return inicio + timedelta(days=random.randint(0, delta))


registros = []
for i in range(1, TOTAL + 1):
    nota = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.08, 0.12, 0.30, 0.45])[0]
    if nota >= 4:
        comentario = random.choice(COMENTARIOS_POS)
    elif nota == 3:
        comentario = random.choice(COMENTARIOS_NEU)
    else:
        comentario = random.choice(COMENTARIOS_NEG)

    data_avaliacao = data_aleatoria(inicio, delta_total)
    updated_at = data_aleatoria(data_avaliacao, (fim - data_avaliacao).days or 1)
    registros.append(
        {
            "id_avaliacao": i,
            "id_pedido": random.randint(1, TOTAL),
            "id_cliente": random.randint(1, TOTAL),
            "id_produto": random.randint(1, TOTAL),
            "nota": nota,
            "comentario": comentario,
            "data_avaliacao": data_avaliacao.isoformat(),
            "updated_at": updated_at.isoformat(),
        }
    )

df = pd.DataFrame(registros)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"avaliacoes.csv gerado: {len(df)} registros")
