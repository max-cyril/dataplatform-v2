from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append("/opt/airflow/ingestion_init")  # pour que le DAG voie le script

from ingestion_init.records import populate_records
from ingestion_init.stream import populate_streams

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}



def populate_both_mysql():
    populate_records(items=100)
    populate_streams(nb_orders=1000)



with DAG(
    dag_id='populate_mysql_dag',
    default_args=default_args,
    description='Popule les bases MySQL records et streams',
    schedule_interval='*/5 * * * *',  # toutes les 5 minutes
    start_date=datetime(2025, 8, 3),
    catchup=False,
    tags=['ingestion'],
) as dag:

    populate_task = PythonOperator(
        task_id='populate_mysql',
        python_callable=populate_both_mysql,
    )
