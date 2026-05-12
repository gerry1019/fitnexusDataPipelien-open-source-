from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
        dag_id="spark_s3_gold_layer_full",
        default_args=default_args,
        start_date=datetime(2024, 1, 1),
        schedule=None,
        catchup=False,
        tags=["gold", "analytics", "fitnexus"],
) as dag:
    # Mapping of domains to their specific scripts
    gold_tables = {
        "operations": ["gold_session_utilization.py"],
        "revenue": ["gold_fc_daily_revenue.py", "gold_platform_revenue_daily.py"],
        "saas": ["gold_platform_subscription_metrics.py", "gold_subscription_growth_daily.py"],
        "user": ["gold_user_growth_daily.py"],
        "funnel": ["gold_booking_funnel_metrics.py"],
        "executive": ["gold_center_business_kpis.py"]
    }

    # Base path inside your spark container
    base_path = "/opt/spark/work-dir/gold-transformation"

    for domain, scripts in gold_tables.items():
        previous_task = None

        for script in scripts:
            task_id = script.replace(".py", "")

            task = BashOperator(
                task_id=task_id,
                bash_command=f"""
                docker exec spark_bronze \
                /opt/spark/bin/spark-submit \
                --master local[*] \
                --driver-memory 1g \
                --executor-memory 1g \
                {base_path}/{script}
                """
            )

            # If a domain has multiple scripts (like Revenue), run them sequentially
            # to prevent memory spikes, but different domains will run in parallel.
            if previous_task:
                previous_task >> task
            previous_task = task