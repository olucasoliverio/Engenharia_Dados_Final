# MongoDB Atlas — origem compartilhada

Para que todos os integrantes acessem a **mesma** base de origem, o MongoDB é
disponibilizado em um cluster gratuito (**M0**) no [MongoDB Atlas](https://www.mongodb.com/atlas).
O mesmo script de carga ([`carregar_mongo.py`](modelo_mongodb.md)) é usado tanto
no Docker local quanto no Atlas — muda apenas o `MONGO_URI`.

!!! warning "Segredos"
    A connection string contém usuário e senha. Ela vai **somente** no `.env`
    local (que está no `.gitignore`) — **nunca** em commits, prints ou no código.

## 1. Criar o cluster

1. Crie a conta / faça login em <https://www.mongodb.com/cloud/atlas/register>.
2. **Create a cluster** → **M0 (Free)**.
3. Provider **AWS**, region **São Paulo (sa-east-1)**.
4. Nomeie (ex.: `ecommerce-origem`) e **Create Deployment**.

## 2. Usuário do banco

Em **Security → Database Access → Add New Database User**:

- Autenticação **Password** (SCRAM).
- Defina um usuário (ex.: `app_user`) e uma senha forte.
- Evite os caracteres `@ : / ? #` na senha (quebram a URI); se usar, faça
  URL-encode na connection string.

## 3. Liberar o acesso de rede

Em **Security → Network Access → + ADD IP ADDRESS**:

- Para o time inteiro acessar de qualquer lugar: **ALLOW ACCESS FROM ANYWHERE**
  (`0.0.0.0/0`) → **Confirm** e aguarde o status **Active**.
- Trade-off: o cluster fica alcançável de qualquer IP, mas o acesso continua
  exigindo usuário e senha. Alternativa mais restrita: cada integrante adiciona
  o próprio IP.

## 4. Obter a connection string

No cluster → **Connect** → **Drivers** → copie a URI:

```
mongodb+srv://app_user:<db_password>@ecommerce-origem.xxxxx.mongodb.net/?retryWrites=true&w=majority
```

Substitua `<db_password>` pela senha real.

## 5. Configurar o `.env`

Cada integrante coloca a URI no **seu** `.env` (a partir do `.env.example`):

```bash
cp .env.example .env
```

```dotenv
MONGO_DB=ecommerce
MONGO_URI=mongodb+srv://app_user:SUASENHA@ecommerce-origem.xxxxx.mongodb.net/?retryWrites=true&w=majority
```

## 6. Carregar os dados no Atlas

Conexões `mongodb+srv://` exigem o `dnspython` (já listado no
`requirements.txt`):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r dataset/scripts_py/requirements.txt

# le o MONGO_URI do .env e carrega as 10 colecoes no Atlas
python dataset/scripts_py/carregar_mongo.py
```

O script é idempotente (recria as coleções) e aplica os mesmos validadores e
índices da carga local. Resultado esperado:

```
Concluido: 150,000 documentos em 10 colecao(oes).
```

## Alternar entre Atlas e local

Basta trocar o `MONGO_URI` no `.env` (ou passar `--uri`):

```bash
# local
python dataset/scripts_py/carregar_mongo.py --uri "mongodb://admin:admin123@localhost:27017/?authSource=admin"
```

## Referências

- [MongoDB Atlas](https://www.mongodb.com/docs/atlas/)
- [PyMongo — conexões SRV](https://pymongo.readthedocs.io/en/stable/examples/high_availability.html)
- Página completa de [referências](referencias.md)
