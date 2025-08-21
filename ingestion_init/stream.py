# scripts/populate_source2.py
import mysql.connector
import random
import uuid
import json
from datetime import datetime
from logger import log_to_es

UUID_CACHE_PATH = "ingestion_init/uuid_cache.json"

def uuid_str_to_binary(uuid_str):
    return uuid.UUID(uuid_str).bytes  # BINARY(16)

@log_to_es(index="records")
def populate_streams(nb_orders=1000):
    # Lire les UUIDs depuis le fichier JSON généré par populate_source1.py
    with open(UUID_CACHE_PATH, "r") as f:
        uuid_cache = json.load(f)

    clients = uuid_cache.get("clients", [])
    products = uuid_cache.get("product", [])

    if not clients or not products:
        raise Exception("uuid_cache.json is missing clients or product data")

    conn = mysql.connector.connect(
        host="mysql_streams",
        port=3306,
        user="root",
        password="root",
        database="streams"
    )
    cursor = conn.cursor()

    for _ in range(nb_orders):
        order_id = uuid.uuid4().bytes
        client = random.choice(clients)
        product = random.choice(products)

        client_id = uuid_str_to_binary(client["id"])
        product_id = uuid_str_to_binary(product["id"])
        quantity = random.randint(1, 5)
        unit_price = product["price"]
        total_price = round(unit_price * quantity, 2)

        cursor.execute("""
            INSERT INTO orders (id, client_id, product_id, quantity, total_price, order_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            order_id,
            client_id,
            product_id,
            quantity,
            total_price,
            datetime.utcnow()
        ))

    conn.commit()
    conn.close()

if __name__ == "__main__":
    populate_streams(nb_orders=1000)
