from airflow.providers.docker.operators.docker import DockerOperator
from airflow import DAG
from airflow.utils.dates import days_ago

with DAG(
    dag_id="dbt_docker_dag",
    start_date=days_ago(1),
    schedule_interval="*/30 * * * *",
    catchup=False,
) as dag:

    run_dbt = DockerOperator(
        task_id="run_dbt",
        image="dbt-dataplatform:1.0.0",
        network_mode="elasticnet",
        api_version='auto',
        auto_remove=True,
        command="dbt build",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        environment={
            "POSTGRE_HOST": "postgres_dwh"}
    )
