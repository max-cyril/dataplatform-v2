# scripts/ingest_source1_to_pg.py
import mysql.connector
import psycopg2
import uuid
from ingestion_init.logger import log_to_es

@log_to_es(index="etl_records")
def records_ingestion():
    # Connexions
    mysql_conn = mysql.connector.connect(
        host="mysql_records",
        port=3306,
        user="root",
        password="root",
        database="records"
    )
    pg_conn = psycopg2.connect(
        host="postgres_dwh",
        user="postgres",
        password="postgres",
        dbname="dwh"
    )
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    # clients
    mysql_cur.execute("SELECT * FROM clients")
    for row in mysql_cur.fetchall():
        pg_cur.execute("""
            INSERT INTO raw.clients (id, name, email, country, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (uuid.UUID(bytes=row["id"]), row["name"], row["email"], row["country"], row["created_at"]))

    # suppliers
    mysql_cur.execute("SELECT * FROM suppliers")
    for row in mysql_cur.fetchall():
        pg_cur.execute("""
            INSERT INTO raw.suppliers (id, name, email, country, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (uuid.UUID(bytes=row["id"]), row["name"], row["email"], row["country"], row["created_at"]))

    # product
    mysql_cur.execute("SELECT * FROM product")
    for row in mysql_cur.fetchall():
        pg_cur.execute("""
            INSERT INTO raw.product (id, name, price, fournisseur_name, created_at)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (uuid.UUID(bytes=row["id"]), row["name"], row["price"], row["fournisseur_name"], row["created_at"]))

    pg_conn.commit()
    mysql_conn.close()
    pg_conn.close()

if __name__ == "__main__":
    records_ingestion()
