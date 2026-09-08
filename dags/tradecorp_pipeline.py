import os
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime, timedelta

ENV_VARS = {
    "account_name": os.getenv("account_name"),
    "account_key": os.getenv("account_key"),
}


default_args = {
    "owner": "tradecorp",
    "retries": 1,                          # réessayer 1 fois en cas d'échec
    "retry_delay": timedelta(minutes=5),   # attendre 5 min avant de réessayer
}

PROJECT_ROOT = "C:\\Users\\hasss\\Desktop\\Data_Engineer\\Semaine_6_Docker_Spark\\tradecorp"

MOUNTS = [
    Mount(source=f"{PROJECT_ROOT}/src", target="/home/jovyan/src", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/data", target="/home/jovyan/data", type="bind"),
    Mount(source=f"{PROJECT_ROOT}/.env", target="/home/jovyan/.env", type="bind"),
]

with DAG(
    dag_id="tradecorp_etl_pipeline",      
    default_args=default_args,
    start_date=datetime(2026, 1, 1),       
    schedule_interval="0 6 * * *",         # cron : tous les jours à 6h
    catchup=False,                          # ne pas rattraper le passé
    tags=["tradecorp", "etl"],
) as dag:

    t0 = BashOperator(
        task_id="fetch_exchange_rates",
        bash_command="python /opt/airflow/src/fetch_exchange_rates.py", 
    )

    t1 = DockerOperator(
        task_id="reader",
        image="tradecorp-spark",
        command="spark-submit /home/jovyan/src/reader.py",      
        docker_url="unix://var/run/docker.sock",
        network_mode="tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=MOUNTS,
        environment=ENV_VARS,
    )

    t2 = DockerOperator(
        task_id="transformer",
        image="tradecorp-spark",
        command="spark-submit /home/jovyan/src/transformer.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=MOUNTS,
        environment=ENV_VARS,
    )

    t3 = DockerOperator(
        task_id="writer",
        image="tradecorp-spark",
        command="spark-submit /home/jovyan/src/writer.py",        
        docker_url="unix://var/run/docker.sock",
        network_mode="tradecorp_default",
        auto_remove=True,
        mount_tmp_dir=False,
        mounts=MOUNTS,
        environment=ENV_VARS,
    )

    t0 >> t1 >> t2 >> t3