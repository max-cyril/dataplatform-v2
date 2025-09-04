from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/opt/airflow") #/ingestion_init") 

from ingestion_microservice.ingestion_streams_job import  streams_ingestion 

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}



def main():
    streams_ingestion()



with DAG(
    dag_id='streams_ingestion_job',
    default_args=default_args,
    description='Ingest streams order from mysql to postgres datawarehouse into raw zone',
    schedule_interval='*/5 * * * *',  # toutes les 5 minutes
    start_date=datetime(2025, 8, 3),
    catchup=False,
    tags=['ingestion'],
) as dag:

    populate_task = PythonOperator(
        task_id='ingestion_streams_job',
        python_callable=main,
    )
