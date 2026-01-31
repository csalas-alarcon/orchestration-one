from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST_JOBS_DIR = r"C:\pmvd-lab\infra\datalake\jobs"
HOST_SPARK_DEFAULTS = r"C:\pmvd-lab\infra\datalake\spark\conf\spark-defaults.conf"

# carpeta que contiene great_expectations.yml + expectations/bronze_vuelos_suite.json
HOST_GE_PROJECT = r"C:\pmvd-lab\infra\datalake\notebooks\gx_test_project"

with DAG(
    dag_id="demo_ge_validate_bronze",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    ge_validate = DockerOperator(
        task_id="spark_submit_ge_validate",
        image="apache/spark:4.0.1",
        auto_remove="success",
        network_mode="datalake",
        user="0:0",
        command=(
            "sh -lc '"
            # 1) instala GE dentro del contenedor del task (modo fácil)
            "python3 -m pip install --no-cache-dir -q \"notebook<7\" && "
            "python3 -m pip install --no-cache-dir -q great-expectations==0.18.22 && "

            # 2) lanza el job con spark-submit
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.driver.extraJavaOptions=-Duser.home=/tmp "
            "--conf spark.executor.extraJavaOptions=-Duser.home=/tmp "
            "/opt/jobs/spark_job_great_expectations_demo.py"
            "'"
        ),
        mount_tmp_dir=False,
        mounts=[
            Mount(source=HOST_JOBS_DIR, target="/opt/jobs", type="bind"),

            Mount(
                source=HOST_SPARK_DEFAULTS,
                target="/opt/spark/conf/spark-defaults.conf",
                type="bind",
                read_only=True,
            ),
            Mount(source="datalake_spark-ivy", target="/opt/spark/ivy", type="volume"),

            # Montamos el proyecto GE como /opt/gx
            Mount(source=HOST_GE_PROJECT, target="/opt/gx", type="bind", read_only=False),
        ],
    )
