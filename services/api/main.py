from fastapi import FastAPI
from pydantic import BaseModel
import joblib, boto3, os, tempfile
import pandas as pd

app = FastAPI(title="Fraud Scoring API")

MODEL = None

def load_model():
    global MODEL
    if MODEL is not None:
        return MODEL
    s3 = boto3.client("s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name="us-east-1"
    )
    with tempfile.NamedTemporaryFile(suffix=".pkl", delete=False) as f:
        s3.download_fileobj(
            "enterprise-streaming-dev-bronze",
            "models/fraud_model/v1/model.pkl",
            f
        )
        MODEL = joblib.load(f.name)
    return MODEL

class Transaction(BaseModel):
    user_id: str
    amount: float
    device_type: str
    country: str
    velocity_10min: int = 0

class ScoreResponse(BaseModel):
    user_id: str
    risk_score: float
    risk_label: str
    decision: str

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/score", response_model=ScoreResponse)
def score(txn: Transaction):
    model = load_model()

    device_map = {"mobile": 0.8, "web": 0.3, "tablet": 0.5}
    country_risk = {"USA": 0, "India": 0, "Germany": 0}

    features = pd.DataFrame([{
        "amount": txn.amount,
        "velocity_10min": txn.velocity_10min,
        "device_risk_score": device_map.get(txn.device_type, 0.5),
        "country_risk_flag": country_risk.get(txn.country, 1),
        "is_high_value": int(txn.amount >= 300),
    }])

    prob = model.predict_proba(features)[0][1]
    label = "HIGH" if prob >= 0.8 else "LOW"
    decision = "BLOCK" if prob >= 0.8 else "ALLOW"

    return ScoreResponse(
        user_id=txn.user_id,
        risk_score=round(float(prob), 4),
        risk_label=label,
        decision=decision
    )
