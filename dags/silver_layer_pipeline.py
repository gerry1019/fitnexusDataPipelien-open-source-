from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="spark_s3_silver_layer",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["silver", "spark", "s3"],
) as dag:

    silver_scripts = [
        "silver_user.py",
        "silver_user_info.py",
        "silver_fitness_center.py",
        "silver_address.py",
        "silver_booking.py",
        "silver_membership_plan.py",
        "silver_fitnesscentermembershipplan.py",
        "silver_fitnesscentersubscription.py",
        "silver_payment.py",
        "silver_session.py",
        "silver_staff.py",
        "silver_user_membership.py",
        "silver_workout_details.py",
    ]

    previous_task = None

    for script in silver_scripts:

        task = BashOperator(
            task_id=script.replace(".py", ""),
            bash_command=f"""
            docker exec spark_bronze \
            /opt/spark/bin/spark-submit \
            --master local[*] \
            --driver-memory 1g \
            --executor-memory 1g \
            /opt/spark/work-dir/silver-transformation/{script}
            """
        )

        if previous_task:
            previous_task >> task

        previous_task = task