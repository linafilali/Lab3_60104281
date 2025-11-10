# Lab 4– Feature Extraction and Enrichment

**Student:** Lina Filali - 60104281

Structure of the repisortory:

├── notebooks/
│ ├── Text feature extraction on Azure.ipynb
├── README.md


## Goal
Enhance the Goodreads **Gold dataset** by creating and integrating advanced text-based, statistical, and semantic features.  
This lab builds on the cleaned data from **Lab 3 (`features_v1`)**, and produces a unified feature matrix under the **Gold layer → `features_v2`**.

---

## I. Environment Setup

### 1. Databricks Runtime
- Runtime: **16.4 LTS (Apache Spark 3.5.2 / Scala 2.12)**
- Worker type: **Standard_D4ds_v5** (16 GB RAM, 4 vCPUs)
- Photon acceleration: **Enabled**
- Autoscaling: **Min 2 – Max 4 workers**

### 2. Azure Data Lake Connection
``
spark.conf.set(
    "fs.azure.account.key.goodreadsreviews60104281.dfs.core.windows.net",
    "<access-key>"
)
``
## II. Data Loading
All inputs were read from the Gold Layer under features_v2:

Dataset	Description	Rows	Columns
text_features	Basic NLP features (cleaned reviews)	10.4 M	21
tfidf_features_clean	TF-IDF matrix (unigrams + bigrams)	10.4 M	1018
bert_embeddings_sample	Semantic embeddings (DistilBERT, sampled)	50 K	15
additional_features	Structural and stylistic indicators	14.9 M	20

## III. Feature Engineering
**Text Features**
- Extracted from review_text to quantify review content:

- review_length_words – total word count

- review_length_chars – total character count

- avg_word_length – average word length per review
``
Saved as:
abfss://lakehouse@goodreadsreviews60104281.dfs.core.windows.net/gold/features_v2/text_features
``
**TF-IDF Features**
- Used sklearn.feature_extraction.text.TfidfVectorizer to capture word importance:

``
tfidf = TfidfVectorizer(
    max_features=1000,
    stop_words="english",
    ngram_range=(1, 2)
)
``
- Vocabulary limited to top 1000 terms

- Removed filler words and rare tokens

- Saved as Delta table under: tfidf_features_clean

**Sentiment Features**
Applied VADER sentiment analysis to compute polarity:

sentiment_pos – proportion of positive tokens

sentiment_neg – proportion of negative tokens

sentiment_neu – proportion of neutral tokens

sentiment_compound – overall sentiment score (−1 to +1)

These features describe the emotional tone and polarity of reviews.

**Semantic Embedding Features**
Generated contextual vector representations using Sentence-BERT (all-MiniLM-L6-v2):

``
from sentence_transformers import SentenceTransformer
model = SentenceTransformer("all-MiniLM-L6-v2")
embeddings = model.encode(sample_texts, show_progress_bar=True)
``

Each embedding (384-D) captures semantic relationships between words and phrases.

Note:
Due to the large dataset size (≈10.4 million reviews), generating embeddings for all rows would take several hours and exceed memory limits.
Therefore, embeddings were computed on a 50,000-row sample to demonstrate the feature extraction process efficiently while maintaining representative quality.
``
Saved as:
abfss://lakehouse@goodreadsreviews60104281.dfs.core.windows.net/gold/features_v2/bert_embeddings_sample
``
**Additional Informative Features**
Statistical and stylistic signals derived from review text:

punctuation_density – ratio of punctuation to characters

num_questions – count of “?” marks

num_exclaims – count of “!” marks

avg_word_length, review_length_words, review_length_chars
``
Saved as:
abfss://lakehouse@goodreadsreviews60104281.dfs.core.windows.net/gold/features_v2/additional_features
``
## IV. Combined Feature Set and Output
Objective
Merge all feature sources into a single feature matrix that combines:

Numerical metrics (length, sentiment, style)

Statistical TF-IDF vectors

Semantic BERT embeddings

Metadata (review_id, book_id, rating)

Implementation
``
combined_df = (
    text_features
    .join(tfidf_features, on="review_id", how="left")
    .join(additional_features, on="review_id", how="left")
    .join(bert_embeddings, on="review_id", how="left")
    .select(
        "review_id",
        "book_id",
        "rating",
        *[c for c in text_features.columns if c not in ["review_id", "book_id", "rating"]]
    )
)
``

``
abfss://lakehouse@goodreadsreviews60104281.dfs.core.windows.net/gold/features_v2/final_features
``

## V. Validation
Validation Step	Description	Result
Schema Check	Verified data types and column names across joins	Passed
Row Count	Confirmed alignment of review_id across tables	Consistent (~10.4 M)
Sample Inspection	Spot-checked merged records and embeddings	Valid non-null values
Gold Save	Confirmed Delta table written successfully	Complete

## VI. Output Structure

``
/gold/features_v2/
├── text_features/
├── tfidf_features_clean/
├── bert_embeddings_sample/
├── additional_features/
└── final_features/
``
