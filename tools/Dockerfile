FROM python:3.11-slim

WORKDIR /app

# Copier tous les scripts
COPY tools/ .

COPY ingestion_microservice/ ingestion_microservice/

# Installer les dépendances
RUN pip install --no-cache-dir \
    mysql-connector-python \
    psycopg2-binary \
    elasticsearch \
    faker

RUN ls -l /app
# Commande d’exécution
CMD ["python", "main.py"]
