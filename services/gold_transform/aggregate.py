import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime

BUCKET = "enterprise-streaming-dev-bronze"

SILVER_PREFIX = "silver/events_parquet/"
GOLD_PREFIX = "gold/daily_metrics/"

s3 = boto3.client("s3")


def list_silver_files():
    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=SILVER_PREFIX
    )

    if "Contents" not in response:
        return []

    return [obj["Key"] for obj in response["Contents"] if obj["Key"].endswith(".parquet")]


def read_parquet_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return pd.read_parquet(BytesIO(obj["Body"].read()))


def write_gold_parquet(df):
    now = datetime.utcnow()

    key = (
        f"{GOLD_PREFIX}"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"daily_metrics.parquet"
    )

    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy")
    buffer.seek(0)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue()
    )

    print(f"Gold metrics written → {key}")


def main():
    print("Starting Gold aggregation...")

    files = list_silver_files()

    if not files:
        print("No Silver files found.")
        return

    dfs = []

    for file_key in files:
        df = read_parquet_from_s3(file_key)
        dfs.append(df)

    df = pd.concat(dfs)

    # -----------------------------
    # Business Aggregations
    # -----------------------------

    daily_metrics = df.groupby("country").agg(
        total_amount=("amount", "sum"),
        transaction_count=("event_id", "count"),
        avg_risk_score=("risk_score", "mean"),
        high_value_count=("is_high_value", "sum")
    ).reset_index()

    write_gold_parquet(daily_metrics)

    print("Gold aggregation completed.")


if __name__ == "__main__":
    main()
