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
```python
spark.sql("SELECT COUNT(*) FROM curated_reviews").show()


**In Microsoft Fabric**

Connected to ADLS using:

let
  Source = AzureStorage.DataLake("https://goodreadsreviews60104281.dfs.core.windows.net/lakehouse/gold/curated_reviews/", [HierarchicalNavigation=true]),
  DeltaTable = DeltaLake.Table(Source)
in
  DeltaTable


- Adjusted types for IDs, ratings, and text fields (COULDNT ADJUST THE DATE FOR THE DATE_ADDED COLUMN)

- Removed missing or invalid records

- Replaced nulls (language = "Unknown", n_votes = 0)

- Trimmed and capitalized text (title, name)

- Created derived aggregations:

- Average rating per BookID

- Review count per BookID

- Average rating per Author
NOTE: WHEN I TRIED TO MERGE THE QUERIES FOR THE THREE AGGREGATIONS INTO THE TABLE , IT WOULDNT WANT TO WORK

- Word count statistics on reviews

- Published final table to Warehouse as curated_reviews

**clean_gold_features.ipynb**

Goal: Complete preprocessing and feature creation in Databricks (Python + PySpark).

**Steps Performed**

**- Connected to ADLS Gen2 via:**

spark.conf.set("fs.azure.account.key.goodreadsreviews60104281.dfs.core.windows.net", "<access-key>")


**- Loaded Silver datasets:**

books = spark.read.parquet(".../processed/books/")
authors = spark.read.parquet(".../processed/authors/")
reviews = spark.read.parquet(".../processed/reviews/")


**Cleaned data:**

- Dropped null review_id, book_id, user_id, and rating

- Filtered invalid ratings (1–5)

- Removed short reviews (<10 chars)

- Trimmed and normalized text

- Deduplicated by review_id

- Added derived features:

- Review length (word count)

- Aggregated metrics per book_id:

features = curated.groupBy("book_id").agg(
    avg("rating").alias("avg_rating_per_book"),
    count("review_id").alias("review_count_per_book")
)


- Saved enriched Gold dataset:

abfss://lakehouse@goodreadsreviews60104281.dfs.core.windows.net/gold/features_v1

**In Fabric**

✔️ Confirmed final curated_reviews table loads successfully
✔️ Verified column types and non-null counts
✔️ Validated published table in Warehouse

**In Databricks**

✔️ features_v1 Delta table schema validated
✔️ Sample query:

spark.read.format("delta").load(".../gold/features_v1").show(5)


