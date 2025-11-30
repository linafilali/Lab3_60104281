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
