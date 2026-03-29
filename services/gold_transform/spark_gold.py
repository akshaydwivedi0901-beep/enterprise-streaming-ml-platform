from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count
import logging

# -------------------------------
# CONFIG
# -------------------------------
SILVER_PATH = "s3a://enterprise-streaming-dev-bronze/silver/events_delta/"
GOLD_PATH = "s3a://enterprise-streaming-dev-bronze/gold/daily_metrics_delta/"


# -------------------------------
# LOGGING SETUP
# -------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


# -------------------------------
# SPARK SESSION BUILDER
# -------------------------------
def create_spark_session(app_name="GoldLayerDelta"):
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


# -------------------------------
# CORE TRANSFORMATION (TESTABLE)
# -------------------------------
def transform_gold(df):
    """
    Performs aggregation on Silver layer data to create Gold metrics.
    """

    df = df.withColumn("is_high_value_int", col("is_high_value").cast("int"))

    gold_df = (
        df.groupBy("country")
        .agg(
            sum("amount").alias("total_amount"),
            count("event_id").alias("transaction_count"),
            avg("risk_score").alias("avg_risk_score"),
            sum("is_high_value_int").alias("high_value_count")
        )
    )

    return gold_df


# -------------------------------
# PIPELINE EXECUTION
# -------------------------------
def run(silver_path=SILVER_PATH, gold_path=GOLD_PATH):
    spark = create_spark_session()

    try:
        logging.info("Reading Silver Delta data...")
        df = spark.read.format("delta").load(silver_path)

        if df.rdd.isEmpty():
            logging.warning("Silver dataset is empty. Skipping Gold aggregation.")
            return

        logging.info("Performing distributed aggregations...")
        gold_df = transform_gold(df)

        logging.info("Writing Gold Delta table...")
        (
            gold_df.write
            .format("delta")
            .mode("overwrite")
            .save(gold_path)
        )

        logging.info("Gold Delta aggregation completed successfully.")

    except Exception as e:
        logging.error(f"Gold job failed: {str(e)}")
        raise

    finally:
        spark.stop()


# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    run()