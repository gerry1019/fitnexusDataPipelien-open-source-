from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="spark_postgres_to_s3_bronze",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bronze", "spark", "s3"],
) as dag:
    run_spark_job = BashOperator(
        task_id="run_spark_bronze_ingestion",
        bash_command="""
        docker exec spark_bronze \
        /opt/spark/bin/spark-submit \
        --master local[*] \
        /opt/spark/work-dir/pipelines/postgres_to_s3_bronze_pipeline.py
        """
    )
