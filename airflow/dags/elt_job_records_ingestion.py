from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/opt/airflow") #/ingestion_init") 

from ingestion_microservice.ingestion_records_job import  records_ingestion 

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}



def main():
    records_ingestion()



with DAG(
    dag_id='records_ingestion_job',
    default_args=default_args,
    description='Ingest records table from mysql to prostgre datawarehouse',
    schedule_interval='*/5 * * * *',  # toutes les 5 minutes
    start_date=datetime(2025, 8, 3),
    catchup=False,
    tags=['ingestion'],
) as dag:

    populate_task = PythonOperator(
        task_id='ingestion_records_job',
        python_callable=main,
    )
