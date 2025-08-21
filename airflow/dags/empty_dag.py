from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello Airflow!")

with DAG(
    dag_id="test_simple_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["test"]
) as dag:

    task1 = PythonOperator(
        task_id="print_hello_task",
        python_callable=print_hello
    )
