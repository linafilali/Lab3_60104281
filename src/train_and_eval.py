import argparse
import json
import os
import time
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, confusion_matrix
import mlflow
import mlflow.sklearn
import joblib


def main(train_path, test_path, selected_features_path, model_dir, metrics_out):
    t0 = time.time()
    print(f"[C] Loading train from {train_path}")
    train_df = pd.read_parquet(train_path)
    print(f"[C] Loading test from {test_path}")
    test_df = pd.read_parquet(test_path)

    with open(selected_features_path, "r") as f:
        selected = json.load(f)["selected_features"]

    print(f"[C] Selected {len(selected)} features from GA")

    X_train = train_df[selected]
    y_train = train_df["label"]
    X_test = test_df[selected]
    y_test = test_df["label"]

    clf = RandomForestClassifier(
        n_estimators=300,
        random_state=42,
        n_jobs=-1,
        class_weight="balanced",
    )

    mlflow.start_run()
    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_param("num_selected_features", len(selected))

    clf.fit(X_train, y_train)

    y_pred = clf.predict(X_test)
    acc = accuracy_score(y_test, y_pred)
    cm = confusion_matrix(y_test, y_pred).tolist()

    metrics = {
        "accuracy": float(acc),
        "num_selected_features": int(len(selected)),
        "num_train_samples": int(len(train_df)),
        "num_test_samples": int(len(test_df)),
        "confusion_matrix": cm,
        "training_time_seconds": float(time.time() - t0),
        "feature_set_version": "1",
        "selected_features": selected,
    }

    mlflow.log_metric("accuracy", acc)
    mlflow.log_dict(metrics, "metrics.json")

    mlflow.sklearn.log_model(
        clf,
        artifact_path="model",
        registered_model_name="tumor_rf_model",
    )

    mlflow.end_run()

    os.makedirs(os.path.dirname(metrics_out), exist_ok=True)
    with open(metrics_out, "w") as f:
        json.dump(metrics, f, indent=2)

    os.makedirs(model_dir, exist_ok=True)
    joblib.dump(clf, os.path.join(model_dir, "model.joblib"))

    print(f"[C] Accuracy: {acc:.4f}")
    print(f"[C] Confusion matrix: {cm}")
    print(f"[C] Metrics saved to {metrics_out}")
    print(f"[C] Model saved to {model_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--train_path", type=str, required=True)
    parser.add_argument("--test_path", type=str, required=True)
    parser.add_argument("--selected_features_path", type=str, required=True)
    parser.add_argument("--model_dir", type=str, required=True)
    parser.add_argument("--metrics_output", type=str, required=True)
    args = parser.parse_args()

    main(
        args.train_path,
        args.test_path,
        args.selected_features_path,
        args.model_dir,
        args.metrics_output,
    )
