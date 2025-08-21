# scripts/ingest_source2_to_pg.py
import mysql.connector
import psycopg2
import uuid
from log_decorator import log_to_es

@log_to_es(index="ingest-source2")
def ingest():
    mysql_conn = mysql.connector.connect(
        host="mysql2",
        user="root",
        password="password",
        database="source2"
    )
    pg_conn = psycopg2.connect(
        host="postgres",
        user="postgres",
        password="password",
        dbname="dwh"
    )
    mysql_cur = mysql_conn.cursor(dictionary=True)
    pg_cur = pg_conn.cursor()

    mysql_cur.execute("SELECT * FROM orders")
    for row in mysql_cur.fetchall():
        pg_cur.execute("""
            INSERT INTO raw.orders (id, client_id, product_id, quantity, total_price, order_date)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """, (
            uuid.UUID(bytes=row["id"]),
            uuid.UUID(bytes=row["client_id"]),
            uuid.UUID(bytes=row["product_id"]),
            row["quantity"],
            row["total_price"],
            row["order_date"]
        ))

    pg_conn.commit()
    mysql_conn.close()
    pg_conn.close()

if __name__ == "__main__":
    ingest()
