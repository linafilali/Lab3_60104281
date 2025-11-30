import json
import os
import pandas as pd
import mlflow


def init():
    """
    Called once when the Azure ML endpoint starts.
    Loads the MLflow model from the model directory.
    """
    global model
    model_dir = os.getenv("AZUREML_MODEL_DIR", ".")
    print(f"Loading model from: {model_dir}")
    model = mlflow.pyfunc.load_model(model_dir)


def run(raw_data):
    """
    Called for each request.

    Expects input JSON like:
    {
      "data": [
        {"f1": 0.1, "f2": 0.2, ...},
        {"f1": 0.5, "f2": 0.7, ...}
      ]
    }
    """
    try:
        data = json.loads(raw_data)
        records = data["data"]
        df = pd.DataFrame.from_records(records)
        preds = model.predict(df)
        return {"predictions": preds.tolist()}
    except Exception as e:
        return {"error": str(e)}
