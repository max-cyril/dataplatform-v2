# scripts/populate_source1.py
import mysql.connector
import random
from faker import Faker
import uuid
import json
import os
from logger import log_to_es

fake = Faker()

UUID_CACHE_PATH = "ingestion_init/uuid_cache.json"

def generate_uuid_binary():
    """Retourne tuple: (uuid_obj, binaire)"""
    u = uuid.uuid4()
    return u, u.bytes

@log_to_es(index="records")
def populate_records(items=100):
    conn = mysql.connector.connect(
        host="mysql_records",
        port=3306,
        user="root",
        password="root",
        database="records"
    )
    
    cursor = conn.cursor()

    uuid_cache = {
        "clients": [],
        "suppliers": [],
        "product": []
    }

    # suppliers
    for _ in range(items):
        uuid_obj, uuid_bin = generate_uuid_binary()
        name = fake.company()
        email = fake.company_email()
        country = fake.country()
        cursor.execute("""
            INSERT INTO suppliers (id, name, email, country)
            VALUES (%s, %s, %s, %s)
        """, (uuid_bin, name, email, country))
        uuid_cache["suppliers"].append({
            "id": str(uuid_obj),
            "name": name
        })

    # clients
    for _ in range(items):
        uuid_obj, uuid_bin = generate_uuid_binary()
        name = fake.name()
        email = fake.email()
        country = fake.country()
        cursor.execute("""
            INSERT INTO clients (id, name, email, country)
            VALUES (%s, %s, %s, %s)
        """, (uuid_bin, name, email, country))
        uuid_cache["clients"].append({
            "id": str(uuid_obj),
            "email": email,
            "name": name
        })

    # product
    suppliers_names = [f["name"] for f in uuid_cache["suppliers"]]
    for _ in range(items):
        uuid_obj, uuid_bin = generate_uuid_binary()
        name = fake.word().capitalize()
        price = round(random.uniform(5, 200), 2)
        fournisseur_name = random.choice(suppliers_names)
        cursor.execute("""
            INSERT INTO product (id, name, price, fournisseur_name)
            VALUES (%s, %s, %s, %s)
        """, (uuid_bin, name, price, fournisseur_name))
        uuid_cache["product"].append({
            "id": str(uuid_obj),
            "name": name,
            "price": price
        })

    # Sauvegarde JSON dynamique

    dir_path = os.path.dirname(UUID_CACHE_PATH)
    if dir_path:
        os.makedirs(dir_path, exist_ok=True)

    with open(UUID_CACHE_PATH, "w") as f:
        json.dump(uuid_cache, f, indent=2)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    populate_records(items=100)
