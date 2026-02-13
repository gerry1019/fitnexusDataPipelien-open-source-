from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pipelines.postgres_ingestion_runner import run_postgres_ingestion

default_args = {
    "owner": "FitNexus",
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "hourly_ingestion",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False
) as dag:

    ingest_hourly = PythonOperator(
        task_id="ingest_hourly_tables",
        python_callable=lambda: run_postgres_ingestion("hourly")
    )
