# Lab 3 – Data Preprocessing on Azure (Databricks + Fabric)

**Student:** Lina Filali - 60104281

Structure of the repisortory:

├── notebooks/
│ ├── Homework Part I.ipynb
│ └── clean_gold_features.ipynb
├── README.md


This lab implements a **modern Lakehouse architecture** using Azure services, following the **Bronze → Silver → Gold** pipeline.  
The goal was to clean, transform, and enrich the **Goodreads Reviews Dataset** using **Azure Data Factory, Databricks (PySpark)**, and **Microsoft Fabric Power Query**.

The work is split into two main notebooks:
- `Homework Part I.ipynb` → Gold Table Curation and Fabric Transformations  
- `clean_gold_features.ipynb` → Databricks Cleaning and Feature Engineering



---

## ⚙️ Architecture Summary

| Layer | Purpose | Tool Used | Output Format |
|--------|----------|------------|----------------|
| **Bronze** | Raw ingestion of JSON files | Azure Data Lake Gen2 | `.json` |
| **Silver** | Transformation to structured Parquet | Azure Data Factory | `.parquet` |
| **Gold** | Clean, enriched analytical data | Databricks / Fabric | `.delta` |

---

## Notebook Details

### `Homework Part I.ipynb`
**Goal:** Curate and clean the **Gold Table** using Spark before publishing to Fabric.

####  Key Steps
- **Join books, authors, and reviews_clean** into a single curated DataFrame  
- **Columns kept:**  
  `review_id`, `book_id`, `title`, `author_id`, `name`, `user_id`, `rating`, `review_text`, `language`, `n_votes`, `date_added`
- **Write to Gold zone as Delta Table:**
abfss://lakehouse@goodreadsreviews60104281.dfs.core.windows.net/gold/curated_reviews

- **Register table** as `curated_reviews` for Fabric SQL queries
- Verified via:
```python spark.sql("SELECT COUNT(*) FROM curated_reviews").show()

I. Fabric Dataflow — Curated Reviews
** Connection Setup**

Connected to Azure Data Lake Storage Gen2 via:

let
  Source = AzureStorage.DataLake("https://goodreadsreviews60104281.dfs.core.windows.net/lakehouse/gold/curated_reviews/", [HierarchicalNavigation=true]),
  DeltaTable = DeltaLake.Table(Source)
in
  DeltaTable

** Data Cleaning & Transformation**

Adjusted types for identifiers (book_id, user_id, author_id) and rating fields.

 The date_added column could not be successfully converted to datetime in Fabric.

Removed records with missing or invalid keys or ratings.

Replaced nulls:

language → "Unknown"

n_votes → 0

Trimmed and standardized capitalization for textual columns (title, name).

Created derived aggregations:

Average rating per book_id

Review count per book_id

Average rating per author

**Word count statistics on reviews**

Note: Attempting to merge multiple aggregated queries into one table caused refresh errors in Fabric.
Final publishing to the Warehouse (curated_reviews) failed, so further cleaning was continued in Databricks.

II. Databricks — Gold Layer: clean_gold_features.ipynb
** Connection Configuration**

Connected to ADLS Gen2 using Spark configuration:

spark.conf.set(
    "fs.azure.account.key.goodreadsreviews60104281.dfs.core.windows.net",
    "<access-key>"
)

**Data Loading**

Loaded Silver layer datasets:

books = spark.read.parquet(".../processed/books/")
authors = spark.read.parquet(".../processed/authors/")
reviews = spark.read.parquet(".../processed/reviews/")

**Data Cleaning Steps**

Dropped nulls from review_id, book_id, user_id, and rating.

Filtered out invalid ratings (must be between 1 and 5).

Removed very short reviews (<10 characters).

Trimmed and normalized text fields.

Removed duplicate reviews (review_id).

**Feature Engineering**

Created key derived features:

review_length → number of words in each review.

Aggregated metrics per book_id:

features = curated.groupBy("book_id").agg(
    avg("rating").alias("avg_rating_per_book"),
    count("review_id").alias("review_count_per_book")
)

**Gold Layer Output**

Saved the final cleaned and enriched dataset:

abfss://lakehouse@goodreadsreviews60104281.dfs.core.windows.net/gold/features_v1

** Validation**
In Microsoft Fabric

✔️ Confirmed the curated_reviews table loads successfully.
✔️ Verified column types and non-null counts.
✔️ Validated table structure in Warehouse preview.

In Databricks

✔️ Confirmed features_v1 Delta schema.
✔️ Sample inspection query:

spark.read.format("delta").load(".../gold/features_v1").show(5)
