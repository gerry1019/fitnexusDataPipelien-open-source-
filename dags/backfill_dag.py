from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipelines.ingestion_utils import run_backfill

default_args = {
    "owner": "FitNexus",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="backfill_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    backfill_task = PythonOperator(
        task_id="run_backfill",
        python_callable=run_backfill
    )
