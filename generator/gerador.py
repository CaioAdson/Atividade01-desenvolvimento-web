import json
import random
import time
from datetime import datetime, timezone


TIPOS_EVENTO = [
    "clique",
    "carrinho",
    "compra",
    "entrega"
]

PRODUTOS = [
    {
        "id": 1,
        "nome": "Notebook",
        "preco": 3500.00
    },
    {
        "id": 2,
        "nome": "Smartphone",
        "preco": 2200.00
    },
    {
        "id": 3,
        "nome": "Fone de ouvido",
        "preco": 250.00
    },
    {
        "id": 4,
        "nome": "Teclado",
        "preco": 180.00
    },
    {
        "id": 5,
        "nome": "Mouse",
        "preco": 100.00
    }
]


def gerar_evento():
    produto = random.choice(PRODUTOS)

    evento = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": random.randint(1, 100),
        "event_type": random.choice(TIPOS_EVENTO),
        "product_id": produto["id"],
        "product_name": produto["nome"],
        "price": produto["preco"]
    }

    return evento


while True:
    evento = gerar_evento()

    print(json.dumps(evento, ensure_ascii=False), flush=True)

    time.sleep(2)