# Lab 5 – MLOps Pipeline for Brain Tumor Detection

## 1. Project Overview

This project implements an end-to-end MLOps pipeline on Azure ML for **brain tumor detection from MRI images**.  
The pipeline follows the **Bronze → Silver → Gold** pattern:

- **Bronze**: image ingestion from local dataset to ADLS Gen2.
- **Silver**: image feature extraction (filters + GLCM) into a Parquet feature table.
- **Silver+ / Feature Store**: registration of the feature set (conceptual; blocked by permissions in my workspace, see notes).
- **Gold**: Azure ML pipeline with:
  - feature retrieval & train/test split,
  - baseline + GA feature selection,
  - model training, registration and deployment to a real-time endpoint.

---

## 2. Repository Structure

```text
.
├── src/
│   ├── ingest_images.py              # Bronze – image ingestion
│   ├── extract_features_component.py # Silver – feature extraction to Parquet
│   ├── register_feature_store.py     # Silver+ – feature store registration (code)
│   ├── feature_retrieval.py          # Gold – load features, join labels, split train/test
│   ├── feature_selection.py          # Gold – baseline + GA feature selection
│   ├── train_and_eval.py             # Gold – training + evaluation + model registration
│   └── score.py                      # Phase 6 – scoring script for real-time endpoint
├── components/
│   ├── feature_retrieval.yml
│   ├── feature_selection.yml
│   └── train_and_eval.yml
├── pipeline_job.py                   # Assembles and submits the Gold pipeline
├── deploy_endpoint.py                # Creates / updates managed online endpoint
├── scripts/
│   └── test_endpoint.py              # Endpoint latency + accuracy test
├── .github/
│   └── workflows/
│       └── aml_pipeline.yml          # GitHub Actions workflow
└── README_lab5.md

```


## 2. Data & Prerequisites

### 2.1 Dataset

The project uses an MRI brain tumor dataset with two classes:

* `yes/` – images with tumor
* `no/` – images without tumor

On the compute instance the dataset is expected at:

```text
./data/brain_tumour_dataset/
    yes/
    no/
```

### 2.2 Azure Resources

* **Subscription:** `a485bb50-61aa-4b2f-bc7f-b6b53539b9d3`
* **Resource group:** `rg-60104281`
* **Azure ML workspace:** `tumour60104281`
* **Storage account:** e.g. `tumour60104281` (ADLS Gen2 enabled)
* **Compute instance / cluster:** e.g. `tumour-cpu-cluster` (Standard_E4ds_v4)

### 2.3 Local / Compute Environment

On the Azure ML compute instance:

* Python **3.10**
* Required packages (installed either manually or via `pip`):

  ```bash
  pip install azure-ai-ml azure-identity mlflow scikit-learn scikit-image scipy pandas numpy tqdm
  ```

---

## 3. How to Run

### 3.1 Prerequisites

Set the following environment variables on the compute instance (or hard-code in the scripts – they are already set in the code in this lab):

* `AZURE_SUBSCRIPTION_ID`
* `AZURE_RESOURCE_GROUP`
* `AZURE_ML_WORKSPACE`

Example:

```bash
export AZURE_SUBSCRIPTION_ID="a485bb50-61aa-4b2f-bc7f-b6b53539b9d3"
export AZURE_RESOURCE_GROUP="rg-60104281"
export AZURE_ML_WORKSPACE="tumour60104281"
```

### 3.2 Bronze – Ingestion

From the Azure ML compute instance in the `lab5` folder:

```bash
python src/ingest_images.py \
  --local-data-dir ./data/brain_tumour_dataset \
  --container-name tumour60104281
```

This script:

* uploads the local dataset into the storage container under:

  * `raw/tumour_images/yes/`
  * `raw/tumour_images/no/`

* is **idempotent** – re-running it will not duplicate existing blobs.

---

### 3.3 Silver – Feature Extraction

```bash
python src/extract_features_component.py \
  --input_folder ./data/brain_tumour_dataset \
  --output_parquet ./data/tumour_features.parquet \
  --num_workers 4
```

This script:

* loads images from `./data/brain_tumour_dataset`,

* converts them to **grayscale**,

* applies multiple filters:

  * entropy (rank entropy with disk kernel)
  * gaussian
  * sobel
  * gabor
  * hessian
  * prewitt

* computes **GLCM (Gray-Level Co-occurrence Matrix)** features for angles **0°, 45°, 90°, 135°** with properties:

  * contrast
  * dissimilarity
  * homogeneity
  * ASM
  * energy
  * correlation

* aggregates many numerical features per image,

* and saves everything into the Silver-layer Parquet file:

```text
./data/tumour_features.parquet
```

Each row contains:

```text
image_id, label, f1, f2, ..., fN
```

---

### 3.4 Silver+ – Register the Silver Table as a Data Asset

Instead of using Azure **Feature Store** (permissions were limited), the Silver Parquet is registered as a **data asset** that can be reused by components.

```bash
python src/register_feature_store.py
```

This registers `tumour_features.parquet` in the Azure ML workspace as:

* **Name:** `tumor_features_silver`
* **Version:** `1`
* **Type:** `uri_file`

It can then be referenced in components and the pipeline as:

```text
azureml:tumor_features_silver:1
```

---

### 3.5 Gold – Pipeline (Feature Retrieval → Feature Selection → Training & Evaluation)

#### 3.5.1 Run the pipeline from the compute instance

```bash
python pipeline_job.py
```

This script:

1. Connects to the Azure ML workspace using `MLClient`.

2. Loads the command components from `components/`:

   * `feature_retrieval.yml`
   * `feature_selection.yml`
   * `train_and_eval.yml`

3. Wires them into a single **pipeline job**:

   * **Component A – Feature Retrieval** (`src/feature_retrieval.py`)

     * Loads Silver features from `azureml:tumor_features_silver:1`
     * Performs **stratified 80/20 train/test split**
     * Outputs:

       * `train.parquet`
       * `test.parquet`

   * **Component B – Feature Selection (Baseline + GA)** (`src/feature_selection.py`)

     * **Baseline method**:

       * Uses a simple filtering approach (correlation / variance threshold).
       * Trains a quick `RandomForestClassifier`.
       * Logs:

         * `baseline_accuracy`
         * `baseline_num_features`
         * to `baseline_metrics.json`
     * **Genetic Algorithm (GA)**:

       * Represents each individual as a **binary mask** over features.
       * Population size ≥ **20**, generations ≥ **10**.
       * Fitness = validation accuracy (optional penalty `-0.001 * num_features`).
       * Outputs:

         * best feature mask
         * list of `selected_feature_names`
         * logs:

           * `ga_accuracy`
           * `ga_num_features`
           * `ga_runtime_seconds`
         * to `ga_metrics.json` and `selected_features.json`.

   * **Component C – Training & Evaluation** (`src/train_and_eval.py`)

     * Loads:

       * `train.parquet`
       * `test.parquet`
       * `selected_features.json`
     * Subsets the data to the GA-selected features.
     * Trains a classifier (in this lab: **RandomForestClassifier**, scikit-learn).
     * Evaluates on the test set.
     * Logs metrics and a confusion matrix.
     * **Registers the model** in Azure ML, e.g. as:

       ```text
       tumor_rf_model
       ```

4. Submits the pipeline job; the run appears in:

   > Azure ML Studio → Jobs → `tumor_gold_pipeline`

You can verify that all three components ran successfully (green check marks).

---

### 3.6 Deployment – Managed Online Endpoint

After the model is registered, deploy it:

```bash
python deploy_endpoint.py
```

This script:

1. Connects to the Azure ML workspace.

2. Finds the **latest version** of the registered model (e.g. `tumor_rf_model` v1).

3. Creates or updates a **Managed Online Endpoint**, for example:

   ```text
   tumor-endpoint-60104281
   ```

4. Creates or updates a **deployment** (e.g. `blue`) using:

   * the registered model
   * scoring script `src/score.py`
   * an Azure ML environment (e.g. `AzureML-sklearn-1.0-ubuntu20.04-py38-cpu@latest`)

5. Routes **100%** of traffic to this deployment.

The endpoint appears under:

> Azure ML Studio → **Endpoints** → **Real-time endpoints** → `tumor-endpoint-XXXX`

---

### 3.7 Testing the Endpoint

From the endpoint’s **Consume** tab, copy:

* **REST endpoint URL**
* **Primary key**

Then on the compute instance:

```bash
export ENDPOINT_URL="https://<your-endpoint-url>/score"
export ENDPOINT_KEY="<your-primary-key>"

python scripts/test_endpoint.py
```

`scripts/test_endpoint.py`:

* iterates over test images,

* encodes each image as base64,

* sends `{"image_b64": "<...>"}` to the endpoint,

* records:

  * prediction
  * true label
  * per-call latency

* computes and prints:

  * overall **accuracy**
  * **average latency**
  * **p95 latency**

* saves aggregated metrics into:

```text
endpoint_test_metrics.json
```

---

## 4. GitHub Actions – Automation (Design & Limitations)

The repository includes a GitHub Actions workflow:

```text
.github/workflows/aml_pipeline.yml
```

### 4.1 Workflow Behaviour (Design)

The workflow is named **“AML Tumor Pipeline & Deployment”** and is configured to:

* **Trigger** on:

  * `push` to branch `MLOp`
  * manual `workflow_dispatch` (run workflow button)
* **Steps:**

  1. `actions/checkout` – fetch repo
  2. `actions/setup-python` – install Python 3.10
  3. Install dependencies:

     ```bash
     pip install azure-ai-ml azure-identity mlflow
     ```
  4. **Azure login** using `azure/login@v2` (service principal / OIDC)
  5. Run:

     ```bash
     python pipeline_job.py
     ```

     to submit the Gold training pipeline.
  6. If successful, run:

     ```bash
     python deploy_endpoint.py
     ```

     to deploy the latest model to the managed endpoint.

Conceptually, this gives a full **CI/CD loop**:
*Git push → pipeline submitted → model trained & registered → endpoint updated.*

### 4.2 Authentication Limitation in the Student Subscription

On the UDST subscription, my Azure role is only **Reader**.
I do **not** have permissions to:

* create an **App Registration / Service Principal**,
* configure **federated credentials (OIDC)**,
* or generate a `clientId` / `clientSecret` for `azure/login`.

Because of this, I cannot populate the GitHub secret `AZURE_CREDENTIALS` with valid service principal details, and the Azure login step fails with:

> “Login failed with Error: Using auth-type: SERVICE_PRINCIPAL. Not all values are present…”

As a workaround:

* The **workflow file is still implemented** to show the intended CI/CD design.
* The **actual training pipeline and deployment are run manually** from the Azure ML compute instance using:

  * `python pipeline_job.py`
  * `python deploy_endpoint.py`
* Screenshots of the **successful pipeline** and **deployed endpoint** are provided in the report / submission.

If in the future a service principal is provided by the course staff, the same workflow can be used by simply adding `AZURE_CREDENTIALS` in the repository’s GitHub Secrets.

---

## 5. Scoring Script – `src/score.py`

At serving time, the endpoint receives a **single MRI image** as base64-encoded bytes.

`score.py` performs:

1. Decode base64 → image array.
2. Preprocess to **grayscale**.
3. Apply **the same filters and GLCM feature extraction** as in `extract_features_component.py`.
4. Load `selected_features.json` (GA output) to keep the **correct feature order**.
5. Load the registered model (`tumor_rf_model`) from the Azure ML model directory.
6. Run the prediction and return a JSON response similar to:

```json
{
  "label": "tumor",        // or "no_tumor"
  "prob_tumor": 0.87       // optional probability
}
```

This ensures **training and inference pipelines are consistent** (same Silver logic).

---

## 6. Short Report

### 6.1 Genetic Algorithm (GA) Approach

* **Representation:** each individual is a binary mask over the available features
  (`1` = feature selected, `0` = feature dropped).
* **Population size:** e.g. **20** individuals.
* **Generations:** e.g. **10**.
* **Operators:**

  * Single-point or uniform crossover between parents.
  * Bit-flip mutation (flip 0/1 with small probability).
* **Fitness function:** validation accuracy of a `RandomForestClassifier` trained on the selected features, with optional penalty:

  [
  \text{fitness} = \text{accuracy} - 0.001 \times \text{num_features}
  ]

This encourages the GA to find a **compact** subset of features that still performs well.

### 6.2 Baseline vs GA

* **Baseline selection:**

  * Simple filter method (e.g. remove low-variance or highly correlated features).
  * Trained a RandomForest on the remaining features.
  * Logged:

    * `baseline_accuracy`
    * `baseline_num_features`

* **GA selection:**

  * Started from all features and searched for a good mask.
  * Logged:

    * `ga_accuracy`
    * `ga_num_features`
    * `ga_runtime_seconds`

**Observation (to be filled with your actual numbers):**

* Baseline kept **X_baseline** features with accuracy **A_baseline**.
* GA kept **X_ga** features with accuracy **A_ga**.
* In my runs, GA **[improved / roughly matched]** accuracy while **reducing** the number of features, which is helpful for faster inference and a simpler model.

### 6.3 Silver Runtime

From the log of `extract_features_component.py`:

* Number of images: **254**
* Total extraction time: **T ≈ 169 s** (example from the run)
* Average time per image ≈ **T / 254 ≈ 0.66 s/image**

The dominant cost is applying multiple filters plus computing GLCM features at four angles for each image.

### 6.4 Compute Usage

* **Compute instance:** `tumour6010481` – `Standard_E4ds_v4`
* Used for:

  * running ingestion & Silver extraction locally,
  * submitting the Gold pipeline,
  * deployment & endpoint tests.
* The Gold pipeline itself uses an Azure ML compute target (CPU), and a full run (feature retrieval, selection, training) typically takes **a few minutes**.

### 6.5 Endpoint Latency (from `scripts/test_endpoint.py`)

(Replace placeholders with your collected metrics.)

* **Average latency:** ~`X` ms
* **p95 latency:** ~`Y` ms
* **Accuracy on test set via endpoint:** `Z` (should be close to the offline test accuracy)

Overall, latency is acceptable for a **real-time medical decision support** use case where sub-second predictions are not strictly required but responsiveness is important.

### 6.6 Final Results

* Final chosen model: **RandomForestClassifier** (scikit-learn) trained on **GA-selected features**.
* Best test accuracy: **[fill your value, e.g. 0.90]**.
* Feature reduction:

  * Original features: **N_total**
  * GA-selected: **N_ga** (e.g. ~40–60% of original)
* Comparison:

  * GA produced a **more compact feature subset** and **[similar / better] accuracy** compared to the baseline filter method.
* End-to-end MLOps:

  * Implemented a full pipeline from **Bronze → Silver → Gold → Deployed endpoint**.
  * Training & deployment can be triggered programmatically (via scripts) and conceptually automated via GitHub Actions.

---

## 7. Example – Calling the Endpoint from Python

```python
import base64
import json
import requests

ENDPOINT_URL = "https://<your-endpoint-url>/score"
ENDPOINT_KEY = "<your-primary-key>"

image_path = "data/brain_tumour_dataset/yes/Y1.jpg"

with open(image_path, "rb") as f:
    b64 = base64.b64encode(f.read()).decode("utf-8")

payload = {"image_b64": b64}

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {ENDPOINT_KEY}",
}

resp = requests.post(ENDPOINT_URL, headers=headers, json=payload)
print(resp.status_code, resp.json())
```

This sends a single MRI image to the managed endpoint and prints the predicted label and probability.

---

> **Note:** Replace placeholder values (**X**, **Y**, **Z**, etc.) with your actual metrics before submission.

```

---

You can paste this into `README.md` in your repo and then just edit the few placeholder numbers (accuracies, feature counts, latencies) once you have them.
::contentReference[oaicite:0]{index=0}
```

