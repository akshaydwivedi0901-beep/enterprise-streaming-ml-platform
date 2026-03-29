from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engg",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="streaming_ml_pipeline",
    default_args=default_args,
    description="Streaming ML pipeline using Spark cluster",
    schedule_interval="@hourly",
    start_date=datetime(2026, 3, 29),
    catchup=False,
) as dag:

    silver_job = BashOperator(
        task_id="bronze_to_silver",
        bash_command="echo 'bronze_to_silver starting...' && exit 0"
    )

    data_validation = BashOperator(
        task_id="data_validation",
        bash_command="""
        python /opt/airflow/dags/../data_quality/validate.py \
        || python /workspaces/enterprise-streaming-ml-platform/data_quality/validate.py
        """,
        env={"SILVER_PATH": "/opt/airflow/data/silver/events_delta/"}
    )

    gold_job = BashOperator(
        task_id="silver_to_gold",
        bash_command="echo 'silver_to_gold starting...' && exit 0"
    )

    silver_job >> data_validation >> gold_job
