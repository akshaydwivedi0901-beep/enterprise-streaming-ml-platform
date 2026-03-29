import pytest
import shutil
import os
from pyspark.sql import SparkSession
from services.gold_transform.spark_gold import transform_gold

TEST_SILVER_PATH = "tmp/silver_test"
TEST_GOLD_PATH = "tmp/gold_test"


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("pytest-delta")
        # ✅ THIS IS THE FIX
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0"
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )
    yield spark
    spark.stop()


def setup_test_data(spark):
    data = [
        ("US", 100, "e1", 0.5, True),
        ("US", 200, "e2", 0.7, False),
        ("IN", 300, "e3", 0.9, True)
    ]

    df = spark.createDataFrame(
        data,
        ["country", "amount", "event_id", "risk_score", "is_high_value"]
    )

    df.write.format("delta").mode("overwrite").save(TEST_SILVER_PATH)


def cleanup():
    if os.path.exists("tmp"):
        shutil.rmtree("tmp")


def test_gold_delta_pipeline(spark):

    cleanup()
    setup_test_data(spark)

    df = spark.read.format("delta").load(TEST_SILVER_PATH)

    result_df = transform_gold(df)

    result_df.write.format("delta").mode("overwrite").save(TEST_GOLD_PATH)

    final_df = spark.read.format("delta").load(TEST_GOLD_PATH)

    result = {row["country"]: row.asDict() for row in final_df.collect()}

    assert result["US"]["total_amount"] == 300
    assert result["IN"]["total_amount"] == 300

    cleanup()