from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id='generate_students_and_jobs',
    default_args=default_args,
    start_date=datetime(2025, 7, 19),
    schedule_interval='*/10 * * * *',  # ⏱️ toutes les 10 min
    catchup=False
) as dag:

    generate_students = DockerOperator(
        task_id='generate_students',
        image='python_scripts',
        container_name='python_scripts',
        api_version='auto',
        auto_remove=False,
        command='python /app/generate_students.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    generate_jobs = DockerOperator(
        task_id='generate_jobs',
        image='python_scripts',
        container_name='python_scripts',
        api_version='auto',
        auto_remove=False,
        command='python /app/generate_jobs.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    generate_students >> generate_jobs
