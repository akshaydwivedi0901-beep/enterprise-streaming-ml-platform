import boto3
import pandas as pd
import numpy as np
import joblib
import json
from io import BytesIO
from datetime import datetime
from sklearn.linear_model import LogisticRegression

# ---------------------------
# CONFIG
# ---------------------------

BUCKET = "enterprise-streaming-dev-bronze"
SILVER_PREFIX = "silver/events_parquet/"
MODEL_PREFIX = "models/fraud_model/"
MODEL_VERSION = "v1"

s3 = boto3.client("s3")


# ---------------------------
# LIST SILVER FILES
# ---------------------------

def list_silver_files():
    response = s3.list_objects_v2(
        Bucket=BUCKET,
        Prefix=SILVER_PREFIX
    )

    if "Contents" not in response:
        return []

    return [
        obj["Key"]
        for obj in response["Contents"]
        if obj["Key"].endswith(".parquet")
    ]


# ---------------------------
# LOAD TRAINING DATA
# ---------------------------

def load_training_data():
    files = list_silver_files()

    if not files:
        raise Exception("No Silver files found for training.")

    dfs = []

    for key in files:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_parquet(BytesIO(obj["Body"].read()))
        dfs.append(df)

    full_df = pd.concat(dfs, ignore_index=True)

    return full_df


# ---------------------------
# TRAIN MODEL
# ---------------------------

def train_model(df):

    required_features = [
        "amount",
        "device_risk_score",
        "country_risk_flag"
    ]

    label_column = "is_high_value"

    # Validate schema
    for col in required_features + [label_column]:
        if col not in df.columns:
            raise Exception(f"Missing required column: {col}")

    df = df.dropna(subset=required_features + [label_column])

    if df.empty:
        raise Exception("No valid rows available for training.")

    X = df[required_features]
    y = df[label_column].astype(int)

    model = LogisticRegression()
    model.fit(X, y)

    return model


# ---------------------------
# SAVE MODEL TO S3
# ---------------------------

def save_model(model):

    model_buffer = BytesIO()
    joblib.dump(model, model_buffer)
    model_buffer.seek(0)

    model_key = f"{MODEL_PREFIX}{MODEL_VERSION}/model.pkl"

    s3.put_object(
        Bucket=BUCKET,
        Key=model_key,
        Body=model_buffer.getvalue()
    )

    metadata = {
        "model_name": "fraud_model",
        "version": MODEL_VERSION,
        "trained_at": datetime.utcnow().isoformat(),
        "features": ["amount", "device_risk_score", "country_risk_flag"]
    }

    metadata_key = f"{MODEL_PREFIX}{MODEL_VERSION}/metadata.json"

    s3.put_object(
        Bucket=BUCKET,
        Key=metadata_key,
        Body=json.dumps(metadata),
        ContentType="application/json"
    )

    print(f"Model saved to s3://{BUCKET}/{MODEL_PREFIX}{MODEL_VERSION}/")


# ---------------------------
# MAIN
# ---------------------------

def main():
    print("Loading Silver data...")
    df = load_training_data()

    print("Training model...")
    model = train_model(df)

    print("Saving model...")
    save_model(model)

    print("Training completed successfully.")


if __name__ == "__main__":
    main()