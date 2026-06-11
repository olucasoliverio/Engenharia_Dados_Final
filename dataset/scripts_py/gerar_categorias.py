import os
import random
import pandas as pd
from faker import Faker
from datetime import datetime, timedelta

fake = Faker("pt_BR")
Faker.seed(10)
random.seed(10)

TOTAL = 15_000
OUTPUT = os.path.join(os.path.dirname(__file__), "..", "arquivos_csv", "categorias.csv")

BASES = [
    "Eletrônicos", "Roupas", "Calçados", "Livros", "Games", "Casa & Jardim",
    "Beleza", "Esportes", "Automotivo", "Brinquedos", "Alimentos", "Pet Shop",
    "Saúde", "Ferramentas", "Informática", "Móveis", "Decoração", "Bebidas",
    "Papelaria", "Música", "Cinema", "Viagem", "Bebê", "Relógios", "Jóias",
    "Ar Livre", "Pesca", "Camping", "Culinária", "Limpeza",
]

DESCRICOES = [
    "Produtos de alta qualidade para o dia a dia",
    "Variedade completa para todas as necessidades",
    "As melhores marcas e preços do mercado",
    "Seleção especial com ótimo custo-benefício",
    "Produtos importados e nacionais",
    "Novidades e lançamentos da temporada",
    "Itens exclusivos e limitados",
    "Coleção completa para todos os gostos",
]

inicio = datetime(2023, 1, 1)
fim = datetime(2026, 6, 11)
delta_total = (fim - inicio).days


def data_aleatoria(inicio, delta):
    return inicio + timedelta(days=random.randint(0, delta))


registros = []
for i in range(1, TOTAL + 1):
    base = random.choice(BASES)
    sufixo = fake.word().capitalize()
    nome = f"{base} - {sufixo}" if i > len(BASES) else base
    updated_at = data_aleatoria(inicio, delta_total)
    registros.append(
        {
            "id_categoria": i,
            "nome_categoria": nome,
            "descricao": random.choice(DESCRICOES),
            "updated_at": updated_at.isoformat(),
        }
    )

df = pd.DataFrame(registros)
os.makedirs(os.path.dirname(OUTPUT), exist_ok=True)
df.to_csv(OUTPUT, index=False, encoding="utf-8")
print(f"categorias.csv gerado: {len(df)} registros")
