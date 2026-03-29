from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import os

logging.basicConfig(level=logging.INFO)

# ✅ Use env var so it works locally AND in AWS
SILVER_PATH = os.getenv(
    "SILVER_PATH",
    "/opt/airflow/data/silver/events_delta/"  # local fallback
)

def run_validation():
    spark = SparkSession.builder \
        .appName("DataValidation") \
        .config("spark.jars.packages", 
                "io.delta:delta-core_2.12:2.4.0") \
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    # ✅ Check if path exists before reading
    if not os.path.exists(SILVER_PATH) and not SILVER_PATH.startswith("s3"):
        logging.warning(f"Silver path {SILVER_PATH} not found — skipping validation")
        spark.stop()
        return

    df = spark.read.format("delta").load(SILVER_PATH)

    errors = []

    if df.filter(col("event_id").isNull()).count() > 0:
        errors.append("Null event_id found")

    if df.filter(col("amount") <= 0).count() > 0:
        errors.append("Invalid amount values")

    if df.filter(col("country").isNull()).count() > 0:
        errors.append("Null country found")

    if errors:
        raise Exception(f"Data validation failed: {errors}")

    logging.info("✅ Data validation passed successfully.")
    spark.stop()


if __name__ == "__main__":
    run_validation()