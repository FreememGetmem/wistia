Absolutely — here’s a **clean, professional README.md** you can drop straight into your repo.
It’s written like a real production data project (clear, technical, but readable).

---

# 🎥 Wistia Video Analytics Pipeline (AWS)

## 📌 Overview

This project implements an **end-to-end data analytics pipeline** for Wistia video engagement data using **AWS serverless and big data services**.
It ingests raw API data, transforms it into an analytics-ready star schema, and enables SQL-based analysis via **Amazon Athena**.

The architecture follows **modern data lake best practices**:

* Raw → Curated layers
* Immutable data
* Partitioned fact tables
* Glue + Spark transformations
* Athena for analytics

---

## 🏗️ Architecture

```
Wistia API
   │
   ▼
AWS Lambda (Ingestion)
   │
   ▼
Amazon S3 (data_raw)
   │
   ▼
AWS Glue Job (PySpark Transform)
   │
   ▼
Amazon S3 (data_curated)
   │
   ▼
AWS Glue Data Catalog
```

---

## 🧱 Technology Stack

| Layer        | Service               |
| ------------ | --------------------- |
| Ingestion    | AWS Lambda            |
| Storage      | Amazon S3             |
| Processing   | AWS Glue (PySpark)    |
| Metadata     | AWS Glue Data Catalog |
| IaC          | AWS CloudFormation    |
| Format       | Parquet               |
| Partitioning | Date-based            |

---

## 📂 S3 Data Layout

### Raw Layer

```
s3://wistia-data/data_raw/
└── media_id=xxx/
    └── ingest_date=YYYY-MM-DD/
        └── *.json
```

### Curated Layer

```
s3://wistia-data/data_curated/
├── dim_media/
│   └── *.parquet
├── dim_visitor/
│   └── *.parquet
└── fact_media_engagement/
    └── date=YYYY-MM-DD/
        └── *.parquet
```

---

## 🧮 Data Model (Star Schema)

### 🟦 DIM_MEDIA

| Column     | Type   |
| ---------- | ------ |
| media_id   | STRING |
| hashed_id  | STRING |
| title      | STRING |
| url        | STRING |
| created_at | STRING |

---

### 🟦 DIM_VISITOR

| Column     | Type   |
| ---------- | ------ |
| visitor_id | STRING |
| ip_address | STRING |
| country    | STRING |

---

### 🟨 FACT_MEDIA_ENGAGEMENT (Partitioned)

| Column           | Type             |
| ---------------- | ---------------- |
| media_id         | STRING           |
| visitor_id       | STRING           |
| play_count       | INT              |
| play_rate        | DOUBLE           |
| total_watch_time | DOUBLE           |
| watched_percent  | DOUBLE           |
| date             | DATE (partition) |

---

## 🔄 Data Processing Logic

### Glue Transform Job

* Reads JSON files from `data_raw`
* Flattens nested structures
* Explodes visitor arrays
* Deduplicates dimensions
* Writes optimized Parquet files
* Partitions fact table by `date`

### Key Transformations

* `explode(visitors)` for visitor dimension
* Star-schema normalization
* Date-based partitioning for Athena performance

---

## 🕷️ Glue Crawler

The Glue Crawler:

* Scans `data_curated`
* Updates schema metadata
* Supports schema evolution

---

## 🚀 Deployment

### Infrastructure

* Provisioned using **CloudFormation**
* Includes:

  * S3 buckets
  * IAM roles
  * Lambda functions
  * Glue jobs
  * Glue crawler

### Execution Flow

1. Lambda ingests Wistia API data
2. Raw JSON stored in S3
3. Glue Spark job transforms data
4. Curated Parquet written to S3
---

## 🔐 Security & IAM

* Least-privilege IAM roles
* Glue job granted:

  * `s3:GetObject`
  * `s3:PutObject`
  * `s3:DeleteObject`
* Separate roles for:

  * Lambda
  * Glue Job
  * Glue Crawler

---

## 📈 Performance Optimizations

* Columnar storage (Parquet)
* Partition pruning (`date`)
* Star schema joins
* Reduced data scanned in Athena
---

## 👤 Author

**Mor Ndour**
Machine Learning & Data Engineer
Cloud | AWS | Spark | MLOps | Analytics
