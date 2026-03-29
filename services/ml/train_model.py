import argparse
import logging
import os
from typing import Optional

import pandas as pd

from joblib import dump
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def read_input(path: str) -> pd.DataFrame:
    logger.info("Reading input from %s", path)
    if path.startswith("s3://"):
        if path.endswith(".parquet") or path.endswith("/"):
            return pd.read_parquet(path, engine="pyarrow")
        return pd.read_csv(path)

    if path.endswith(".parquet"):
        return pd.read_parquet(path)

    return pd.read_csv(path)


def prepare_features(df: pd.DataFrame, target_col: str):
    if target_col not in df.columns:
        raise KeyError(f"Target column '{target_col}' not found in data")

    y = df[target_col]
    X = df.drop(columns=[target_col])

    # simple preprocessing: one-hot encode categorical columns
    obj_cols = X.select_dtypes(include=["object", "category"]).columns.tolist()
    if obj_cols:
        X = pd.get_dummies(X, columns=obj_cols, drop_first=True)

    return X, y


def train(X, y, test_size: float = 0.2, random_state: int = 42, n_estimators: int = 100, max_depth: Optional[int] = None):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)

    model = RandomForestClassifier(n_estimators=n_estimators, max_depth=max_depth, random_state=random_state)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    report = classification_report(y_test, preds, output_dict=False)
    logger.info("Training complete. Evaluation:\n%s", report)

    return model


def save_model(model, path: str):
    if path.endswith("/") or os.path.isdir(path):
        os.makedirs(path, exist_ok=True)
        out_file = os.path.join(path, "model.joblib")
    else:
        out_file = path

    if not out_file.endswith(".joblib"):
        out_file = out_file + ".joblib"

    dump(model, out_file)
    logger.info("Model saved to %s", out_file)


def parse_args():
    p = argparse.ArgumentParser(description="Train a simple scikit-learn model")
    p.add_argument("input", help="Input dataset path (csv or parquet; local or s3://)")
    p.add_argument("--target", required=True, help="Target column name")
    p.add_argument("--output", required=True, help="Output path for saved model (file or dir)")
    p.add_argument("--test-size", type=float, default=0.2)
    p.add_argument("--random-state", type=int, default=42)
    p.add_argument("--n-estimators", type=int, default=100)
    p.add_argument("--max-depth", type=int, default=None)
    return p.parse_args()


def main():
    args = parse_args()

    df = read_input(args.input)
    X, y = prepare_features(df, args.target)
    model = train(X, y, test_size=args.test_size, random_state=args.random_state, n_estimators=args.n_estimators, max_depth=args.max_depth)
    save_model(model, args.output)


if __name__ == "__main__":
    main()
