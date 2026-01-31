from airflow.sdk import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

HOST_JOBS_DIR = "/opt/airflow/jobs"

@dag(
   dag_id="Spark_direct_submit",
   schedule=None,
   catchup=False
)

def my_dag():
   read_data = SparkSubmitOperator(
       task_id="read_data",
       application=HOST_JOBS_DIR+"/spark_direct_submit_demo.py",
       conn_id="spark_connection",
       verbose=True
   )
   read_data

my_dag()