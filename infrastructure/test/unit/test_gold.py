import pytest
from pyspark.sql import SparkSession
from services.gold_transform.spark_gold import transform_gold


@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("pytest-gold") \
        .getOrCreate()
    yield spark
    spark.stop()


def test_gold_aggregation(spark):

    data = [
        ("US", 100, "e1", 0.5, True),
        ("US", 200, "e2", 0.7, False),
        ("IN", 300, "e3", 0.9, True)
    ]

    df = spark.createDataFrame(
        data,
        ["country", "amount", "event_id", "risk_score", "is_high_value"]
    )

    result = transform_gold(df)

    result_data = {
        row["country"]: row.asDict()
        for row in result.collect()
    }

    # Assertions
    assert result_data["US"]["total_amount"] == 300
    assert result_data["US"]["transaction_count"] == 2
    assert result_data["US"]["high_value_count"] == 1

    assert result_data["IN"]["total_amount"] == 300
    assert result_data["IN"]["transaction_count"] == 1