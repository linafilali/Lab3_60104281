import json
import os
import io
import base64
import numpy as np
import pandas as pd
import mlflow

from PIL import Image

from skimage import img_as_ubyte
from skimage.filters.rank import entropy
from skimage.morphology import disk
from scipy import ndimage as nd
from skimage.filters import sobel, gabor, hessian, prewitt
from skimage.feature import graycomatrix, graycoprops

# Globals
model = None
selected_feature_names = None  # list of feature names selected by GA (optional)


# ---------- Helpers ----------

def _load_selected_features(model_dir: str):
    """Try to load selected_features.json from the model directory."""
    path = os.path.join(model_dir, "selected_features.json")
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                data = json.load(f)
            if isinstance(data, dict) and "selected_features" in data:
                return data["selected_features"]
            if isinstance(data, list):
                return data
        except Exception as e:
            print(f"Could not load selected_features.json: {e}")
    return None


def _decode_image_from_b64(b64_str: str) -> np.ndarray:
    """Decode base64 string to grayscale numpy array in [0,1]."""
    img_bytes = base64.b64decode(b64_str)
    pil_img = Image.open(io.BytesIO(img_bytes)).convert("L")  # grayscale
    arr = np.array(pil_img).astype(np.float32)
    if arr.max() > 0:
        arr = arr / 255.0
    return arr


def _glcm_features(img_u8: np.ndarray, prefix: str) -> dict:
    """Compute GLCM features (contrast, etc.) and average over angles."""
    distances = [1]
    angles = [0, np.pi / 4, np.pi / 2, 3 * np.pi / 4]
    glcm = graycomatrix(
        img_u8,
        distances=distances,
        angles=angles,
        levels=256,
        symmetric=True,
        normed=True,
    )
    props = ["contrast", "dissimilarity", "homogeneity", "ASM", "energy", "correlation"]

    feats = {}
    for p in props:
        vals = graycoprops(glcm, p)  # (len(distances), len(angles))
        feats[f"{prefix}_{p}_mean"] = float(vals.mean())
    return feats


def _extract_silver_features(gray: np.ndarray) -> dict:
    """
    Reproduce Silver-layer features on a single grayscale image:
    entropy, gaussian, sobel, gabor, hessian, prewitt + their GLCM stats.
    """
    base_u8 = img_as_ubyte(np.clip(gray, 0.0, 1.0))

    ent_img = entropy(base_u8, disk(2))
    gauss_img = nd.gaussian_filter(gray, sigma=1)
    sobel_img = sobel(gray)
    gabor_img = gabor(gray, frequency=0.9)[1]  # imaginary part
    hessian_img = hessian(gray, sigmas=range(1, 3, 1))
    prewitt_img = prewitt(gray)

    feature_dict = {}
    feature_dict.update(_glcm_features(base_u8, "base"))
    feature_dict.update(_glcm_features(img_as_ubyte(ent_img), "entropy"))
    feature_dict.update(_glcm_features(img_as_ubyte(gauss_img), "gaussian"))
    feature_dict.update(_glcm_features(img_as_ubyte(sobel_img), "sobel"))
    feature_dict.update(_glcm_features(img_as_ubyte(gabor_img), "gabor"))
    feature_dict.update(_glcm_features(img_as_ubyte(hessian_img), "hessian"))
    feature_dict.update(_glcm_features(img_as_ubyte(prewitt_img), "prewitt"))

    return feature_dict


def _predict_label_and_proba(df: pd.DataFrame):
    """Run model and return ('tumor'/'no_tumor', prob_tumor or None)."""
    preds = model.predict(df)
    label_num = int(preds[0])
    label_str = "tumor" if label_num == 1 else "no_tumor"

    prob = None
    try:
        # Try to access predict_proba in various ways
        if hasattr(model, "predict_proba"):
            proba = model.predict_proba(df)[0]
            if len(proba) == 2:
                prob = float(proba[1])
        elif hasattr(model, "_model_impl") and hasattr(model._model_impl, "predict_proba"):
            proba = model._model_impl.predict_proba(df)[0]
            if len(proba) == 2:
                prob = float(proba[1])
    except Exception as e:
        print(f"Could not compute probability: {e}")

    return label_str, prob


# ---------- Azure ML entry points ----------

def init():
    """
    Called once when the endpoint starts.
    Loads the MLflow model and selected feature list (if available).
    """
    global model, selected_feature_names

    model_dir = os.getenv("AZUREML_MODEL_DIR", ".")
    print(f"[score.py] Loading model from: {model_dir}")
    model = mlflow.pyfunc.load_model(model_dir)

    selected_feature_names = _load_selected_features(model_dir)
    if selected_feature_names:
        print(f"[score.py] Using {len(selected_feature_names)} GA-selected features.")
    else:
        print("[score.py] No selected_features.json, will use all computed features.")


def run(raw_data):
    """
    Called per HTTP request.

    Expects JSON:
    {
      "image_b64": "<base64-encoded MRI image>"
    }
    """
    try:
        data = json.loads(raw_data)
        if "image_b64" not in data:
            return {"error": "Request JSON must contain 'image_b64'."}

        # 1) Decode image
        gray = _decode_image_from_b64(data["image_b64"])

        # 2) Extract Silver features
        all_feats = _extract_silver_features(gray)

        # 3) Keep only GA-selected features (if list exists)
        if selected_feature_names:
            row = {name: all_feats.get(name, 0.0) for name in selected_feature_names}
        else:
            row = dict(sorted(all_feats.items()))

        df = pd.DataFrame([row])

        # 4) Predict
        label_str, prob = _predict_label_and_proba(df)

        resp = {"prediction": label_str}
        if prob is not None:
            resp["prob_tumor"] = prob

        return resp

    except Exception as e:
        return {"error": str(e)}
