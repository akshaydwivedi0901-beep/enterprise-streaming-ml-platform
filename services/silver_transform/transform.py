import boto3
import pandas as pd
import json
from io import BytesIO
from datetime import datetime

# ---------------------------
# CONFIG
# ---------------------------

BUCKET = "enterprise-streaming-dev-bronze"

BRONZE_PREFIX = "bronze/enriched/"
SILVER_PREFIX = "silver/events_parquet/"
CHECKPOINT_KEY = "silver/_checkpoint/processed_files.json"

s3 = boto3.client("s3")


# ---------------------------
# CHECKPOINT MANAGEMENT
# ---------------------------

def load_checkpoint():
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=CHECKPOINT_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return []
    except Exception:
        return []


def save_checkpoint(processed_files):
    s3.put_object(
        Bucket=BUCKET,
        Key=CHECKPOINT_KEY,
        Body=json.dumps(processed_files),
        ContentType="application/json"
    )


# ---------------------------
# LIST BRONZE FILES
# ---------------------------

def list_bronze_files():
    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=BRONZE_PREFIX
    )

    if "Contents" not in response:
        return []

    return [
        obj["Key"]
        for obj in response["Contents"]
        if obj["Key"].endswith(".json")
    ]


# ---------------------------
# READ JSON
# ---------------------------

def read_json_from_s3(key):
    obj = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


# ---------------------------
# FEATURE ENGINEERING
# ---------------------------

def enrich_features(df):

    device_risk_map = {
        "mobile": 0.3,
        "desktop": 0.1,
        "tablet": 0.2
    }

    df["device_risk_score"] = df["device_type"].map(device_risk_map).fillna(0.2)

    high_risk_countries = ["Nigeria", "Russia"]

    df["country_risk_flag"] = df["country"].isin(high_risk_countries).astype(int)

    return df


# ---------------------------
# WRITE SILVER PARQUET
# ---------------------------

def write_parquet_to_s3(df):

    now = datetime.utcnow()

    key = (
        f"{SILVER_PREFIX}"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"events_{int(now.timestamp())}.parquet"
    )

    buffer = BytesIO()
    df.to_parquet(buffer, engine="pyarrow", compression="snappy")
    buffer.seek(0)

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=buffer.getvalue()
    )

    print(f"Silver written → {key}")


# ---------------------------
# MAIN
# ---------------------------

def main():
    print("Starting incremental Silver transformation...")

    processed_files = load_checkpoint()
    bronze_files = list_bronze_files()

    new_files = [f for f in bronze_files if f not in processed_files]

    if not new_files:
        print("No new Bronze files to process.")
        return

    records = []

    for file_key in new_files:
        record = read_json_from_s3(file_key)
        records.append(record)

    df = pd.DataFrame(records)

    # ---------------------------
    # REQUIRED SCHEMA VALIDATION
    # ---------------------------

    required_columns = [
        "event_id",
        "user_id",
        "amount",
        "device_type",
        "country",
        "timestamp",
        "risk_score",
        "is_high_value"
    ]

    missing_cols = [col for col in required_columns if col not in df.columns]

    if missing_cols:
        raise Exception(f"Missing required columns in Bronze data: {missing_cols}")

    df = df[required_columns]

    # ---------------------------
    # TYPE ENFORCEMENT
    # ---------------------------

    df["amount"] = df["amount"].astype(float)
    df["risk_score"] = df["risk_score"].astype(float)
    df["is_high_value"] = df["is_high_value"].astype(bool)

    # ---------------------------
    # DEDUPLICATION
    # ---------------------------

    df = df.drop_duplicates(subset=["event_id"])

    # ---------------------------
    # FEATURE ENGINEERING
    # ---------------------------

    df = enrich_features(df)

    # ---------------------------
    # WRITE TO SILVER
    # ---------------------------

    write_parquet_to_s3(df)

    # ---------------------------
    # UPDATE CHECKPOINT
    # ---------------------------

    updated_checkpoint = processed_files + new_files
    save_checkpoint(updated_checkpoint)

    print("Incremental Silver transformation completed.")


if __name__ == "__main__":
    main()