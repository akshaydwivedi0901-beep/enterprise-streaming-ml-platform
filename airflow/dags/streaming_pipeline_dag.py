from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import os

default_args = {
    "owner": "data-engg",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

PROJECT_DIR = "/workspaces/enterprise-streaming-ml-platform"
SPARK_IMAGE = "enterprise-streaming-ml-platform-spark:latest"
SPARK_MASTER = "spark://spark:7077"
NETWORK = "enterprise-streaming-ml-platform_default"
AWS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", "")
BUCKET = "enterprise-streaming-dev-bronze"

def spark_cmd(script):
    return (
        f"docker run --rm "
        f"--network {NETWORK} "
        f"-e AWS_ACCESS_KEY_ID={AWS_KEY} "
        f"-e AWS_SECRET_ACCESS_KEY={AWS_SECRET} "
        f"-e AWS_DEFAULT_REGION=us-east-1 "
        f"-v {PROJECT_DIR}:/opt/jobs "
        f"{SPARK_IMAGE} "
        f"/opt/spark/bin/spark-submit "
        f"--master {SPARK_MASTER} "
        f"--packages io.delta:delta-spark_2.12:3.0.0,"
        f"org.apache.hadoop:hadoop-aws:3.3.4,"
        f"com.amazonaws:aws-java-sdk-bundle:1.12.262 "
        f"--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        f"--conf spark.sql.catalog.spark_catalog="
        f"org.apache.spark.sql.delta.catalog.DeltaCatalog "
        f"--conf spark.hadoop.fs.s3a.access.key={AWS_KEY} "
        f"--conf spark.hadoop.fs.s3a.secret.key={AWS_SECRET} "
        f"--conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com "
        f"/opt/jobs/{script}"
    )

def check_silver_not_empty(**context):
    import boto3
    try:
        s3 = boto3.client("s3",
            aws_access_key_id=AWS_KEY,
            aws_secret_access_key=AWS_SECRET,
            region_name="us-east-1"
        )
        r = s3.list_objects_v2(Bucket=BUCKET, Prefix="silver/", MaxKeys=1)
        if "Contents" in r:
            return "silver_to_gold"
    except Exception as e:
        print(f"S3 check failed: {e}")
    return "skip_gold"

with DAG(
    dag_id="streaming_ml_pipeline",
    default_args=default_args,
    description="Bronze → Silver → Gold → ML Training",
    schedule_interval="@hourly",
    start_date=datetime(2026, 3, 29),
    catchup=False,
    tags=["streaming", "fraud", "ml"],
) as dag:

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver",
        bash_command=spark_cmd("services/silver_transform/spark_silver.py"),
        execution_timeout=timedelta(minutes=30),
    )

    data_validation = BashOperator(
        task_id="data_validation",
        bash_command=spark_cmd("data_quality/validate.py"),
        execution_timeout=timedelta(minutes=15),
    )

    check_silver = BranchPythonOperator(
        task_id="check_silver_has_data",
        python_callable=check_silver_not_empty,
        provide_context=True,
    )

    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=spark_cmd("services/gold_transform/spark_gold.py"),
        execution_timeout=timedelta(minutes=30),
    )

    skip_gold = BashOperator(
        task_id="skip_gold",
        bash_command='echo "Silver empty — Gold skipped."',
    )

    ml_training = BashOperator(
        task_id="ml_training",
        bash_command=(
            f"docker run --rm "
            f"--network {NETWORK} "
            f"-e AWS_ACCESS_KEY_ID={AWS_KEY} "
            f"-e AWS_SECRET_ACCESS_KEY={AWS_SECRET} "
            f"-e AWS_DEFAULT_REGION=us-east-1 "
            f"-v {PROJECT_DIR}:/opt/jobs "
            f"python:3.11-slim bash -c '"
            f"pip install boto3 pandas scikit-learn joblib pyarrow -q && "
            f"python /opt/jobs/services/ml_training/train.py'"
        ),
        trigger_rule=TriggerRule.NONE_FAILED,
        execution_timeout=timedelta(minutes=20),
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command='echo "Pipeline completed at $(date)"',
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    bronze_to_silver >> data_validation >> check_silver
    check_silver >> [silver_to_gold, skip_gold]
    [silver_to_gold, skip_gold] >> ml_training >> notify_success
