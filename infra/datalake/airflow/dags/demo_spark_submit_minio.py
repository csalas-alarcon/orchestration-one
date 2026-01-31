from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

HOST_JOBS_DIR = r"C:\pmvd-lab\infra\datalake\jobs"  # <- ajusta si tu ruta difiere

# Ruta del spark-defaults en tu host (Windows)
HOST_SPARK_DEFAULTS = r"C:\pmvd-lab\infra\datalake\spark\conf\spark-defaults.conf"

with DAG(
    dag_id="demo_spark_submit_minio",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    run_spark = DockerOperator(
        task_id="spark_submit_job",
        image="apache/spark:4.0.1",
        auto_remove="success",
        network_mode="datalake",
        user="0:0",
        command=(
            "sh -lc '"
            "/opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "--conf spark.driver.extraJavaOptions=-Duser.home=/tmp "
            "--conf spark.executor.extraJavaOptions=-Duser.home=/tmp "
            "/opt/jobs/spark_job_read_minio_demo.py"
            "'"
        ),
        mount_tmp_dir=False,
        mounts=[
            # Jobs
            Mount(source=HOST_JOBS_DIR, target="/opt/jobs", type="bind"),

            # Spark defaults (S3A + Delta + warehouse + ivy dir + etc.)
            Mount(
                source=HOST_SPARK_DEFAULTS,
                target="/opt/spark/conf/spark-defaults.conf",
                type="bind",
                read_only=True,
            ),
            # Ivy cache persistente (ojo: el driver es ESTE contenedor)
            Mount(source="datalake_spark-ivy", target="/opt/spark/ivy", type="volume"),
        ],
    )
