import os
import time
import json
import base64
import requests
import numpy as np
from pathlib import Path

# ===== CONFIG =====
# Set these two from your endpoint "Consume" tab
ENDPOINT_URL = os.getenv("ENDPOINT_URL", "https://<your-endpoint-url>/score")
ENDPOINT_KEY = os.getenv("ENDPOINT_KEY", "<your-key-here>")

# Test images (you can change to a separate test split if you have one)
DATA_ROOT = Path("./data/brain_tumour_dataset")
YES_DIR = DATA_ROOT / "yes"
NO_DIR = DATA_ROOT / "no"
# ==================


def load_images_with_labels():
    samples = []

    for path in YES_DIR.glob("*"):
        if path.is_file():
            samples.append((path, 1))  # tumor

    for path in NO_DIR.glob("*"):
        if path.is_file():
            samples.append((path, 0))  # no_tumor

    return samples


def encode_image_b64(path: Path) -> str:
    with open(path, "rb") as f:
        img_bytes = f.read()
    return base64.b64encode(img_bytes).decode("utf-8")


def call_endpoint(image_b64: str):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {ENDPOINT_KEY}",
    }
    payload = {"image_b64": image_b64}

    t0 = time.time()
    resp = requests.post(ENDPOINT_URL, headers=headers, data=json.dumps(payload))
    latency = time.time() - t0

    try:
        data = resp.json()
    except Exception:
        data = {"error": resp.text}

    return data, latency


def main():
    samples = load_images_with_labels()
    if not samples:
        print("No images found in test folders.")
        return

    latencies = []
    correct = 0

    for i, (path, true_label) in enumerate(samples, 1):
        img_b64 = encode_image_b64(path)
        result, latency = call_endpoint(img_b64)
        latencies.append(latency)

        if "error" in result:
            print(f"[{i}/{len(samples)}] ERROR for {path.name}: {result['error']}")
            continue

        pred_str = result.get("prediction", "no_tumor")
        pred_label = 1 if pred_str == "tumor" else 0

        if pred_label == true_label:
            correct += 1

        print(
            f"[{i}/{len(samples)}] {path.name} -> "
            f"pred={pred_str}, true={'tumor' if true_label == 1 else 'no_tumor'}, "
            f"latency={latency*1000:.1f} ms"
        )

    latencies = np.array(latencies)
    avg_latency = float(latencies.mean())
    p95_latency = float(np.percentile(latencies, 95))
    accuracy = correct / len(samples)

    print("\n===== Endpoint Test Summary =====")
    print(f"Num calls:        {len(samples)}")
    print(f"Accuracy:         {accuracy:.3f}")
    print(f"Avg latency:      {avg_latency*1000:.1f} ms")
    print(f"p95 latency:      {p95_latency*1000:.1f} ms")

    summary = {
        "num_calls": len(samples),
        "accuracy": accuracy,
        "avg_latency_ms": avg_latency * 1000,
        "p95_latency_ms": p95_latency * 1000,
    }
    with open("endpoint_test_metrics.json", "w") as f:
        json.dump(summary, f, indent=2)
    print("Saved metrics to endpoint_test_metrics.json")


if __name__ == "__main__":
    main()
