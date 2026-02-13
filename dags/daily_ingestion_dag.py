import sys
sys.path.append("/opt/airflow")
sys.path.append("/opt/airflow/pipelines")
sys.path.append("/opt/airflow/utils")

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.postgres_ingestion_runner import run_postgres_ingestion

with DAG(
    dag_id="daily_ingestion",
    start_date=datetime(2025, 1, 1),
    schedule_interval="0 0 * * *",
    catchup=False,
    tags=["bronze", "daily"],
) as dag:

    daily_ingest = PythonOperator(
        task_id="daily_ingest_task",
        python_callable=run_postgres_ingestion,
    )
