from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Make sure this import path matches your actual project structure
from pipelines.postgres_to_s3_bronze_pipeline import run_full_ingestion

default_args = {
    "owner": "fitnexus-daily",
    "depends_on_past": False,
    # It is good practice to keep start_date static (don't use datetime.now())
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_to_s3_bronze_incremental_ingestion",
    description="Incremental ingestion from Postgres → MinIO Bronze",
    default_args=default_args,
    # Runs once every 24 hours (at midnight UTC by default)
    schedule_interval="@daily",
    # Prevents Airflow from running all the past missing days
    catchup=False,
) as dag:

    PythonOperator(
        task_id="ingest_all_tables_sequentially",
        python_callable=run_full_ingestion,
    )