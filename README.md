# ISIT312 Big Data Management — Techno Store Middle East LLC
### Data Lake Processing Pipeline & Analytical Jobs (Coding Part 2)
**Student Name:** Wissam Musba  
**Student ID:** 8973465  
**Course:** ISIT312 Big Data Management (Autumn 2026)  
**Instructor:** Dr. Patrick Mukala  

---

## 1. Project Overview

This repository contains the data engineering pipeline and analytical jobs implemented for **Techno Store Middle East LLC**, an e-commerce retail platform based in the United Arab Emirates specializing in professional audio-visual equipment, musical instruments, and studio hardware.

The project implements a centralized **Hadoop Data Lake architecture** (HDFS, Apache Spark, and Apache Hive) following the Medallion / multi-hop pattern (Bronze, Silver, and Gold zones). 

Within our 7-member group project work distribution:
* **Member 5 (Anusha)** owns **Coding Part 1**: HDFS folder structuring, raw CSV ingestion, and initial Hive schema setup.
* **Member 6 (Wissam Musba)** owns **Coding Part 2**: Automated data processing pipeline, Stage 3 data cleansing / standardisation (Bronze to Silver), Stage 4 analytical jobs (Silver to Gold), and Hive Parquet table curation.

---

## 2. Architecture & Data Lake Zones

The data pipeline runs on a distributed Hadoop ecosystem:

```
[Raw Sources: WooCommerce Catalog (5,929 rows) & Monthly Sales (13 months)]
                                  │
                                  ▼
      [Bronze Zone: HDFS /datalake/raw/woocommerce/.../ingest_date=YYYY-MM-DD/]
                                  │
                                  ▼ (Stage 3: Spark Data Cleansing & Normalization)
      [Silver Zone: HDFS /datalake/cleansed/ & /datalake/cleansed/_quarantine/]
                                  │
                                  ▼ (Stage 4: Star Schema Curation & Business Analytics)
      [Gold Zone: HDFS /datalake/curated/runs/run_001/ (Parquet)]
        ├── dim_product         (5,929 products, cleaned prices, imputed SKUs)
        ├── dim_brand           (155 unique brands)
        ├── dim_category        (398 normalized category paths)
        ├── dim_date            (13 monthly time records)
        └── fact_sales_monthly  (13 monthly sales aggregates)
                                  │
                                  ▼
      [Consumption: Apache Hive Metastore / HiveServer2 / Beeline SQL]
```

---

## 3. Stage 3: Data Cleansing & Transformation Logic

Data profiling of the raw WooCommerce export revealed several data quality issues that had to be resolved before analytical querying:

1. **HTML Web Encoding Defects**: Brand and product names contained unescaped HTML entities (such as `&amp;`). A text replacement parser resolved **519 HTML entity errors**, ensuring brands like *Allen & Heath* are treated as a single unified entity.
2. **Missing SKU Imputation**: 25 products lacked an SKU in the export. Rather than dropping valid inventory items, automated surrogate keys (`AUTO-GEN-<Product_ID>`) were generated to preserve referential integrity.
3. **Regular Price Normalization**: Cast regular prices from text to standard decimal values in UAE Dirhams (AED) and handled 44 missing / blank price fields safely.
4. **Category Taxonomy Flattening**: Simplified deep nested categories into distinct root business domains (e.g., Consumer Audio, Lighting, Networking).
5. **Sales Metric Correction**: Fixed the misleading static average sales amount column in the raw export by calculating the genuine monthly Average Order Value (AOV = Net Sales / Orders).

---

## 4. Stage 4: Sample Analytical Jobs & Business Findings

Three analytical jobs were executed using PySpark DataFrame operations on the curated Parquet tables:

* **Job 1: Brand Volume & Pricing Analytics**  
  Aggregates inventory count and calculates average and maximum prices per brand.  
  *Finding:* Yamaha (418 items), RCF (268 items), and Tannoy (251 items) represent the highest catalogue presence, while Soundcraft represents the highest-value studio tier (average regular price of 25,480.88 AED).

* **Job 2: Product Price Tier Distribution**  
  Classifies products into 5 market price tiers.  
  *Finding:* Over 51.1% of the catalogue (3,032 products) falls in the Mid-Range tier (1,000–5,000 AED), while Professional (5,000–15,000 AED) and High-End Studio (>15,000 AED) represent 24.2% and 12.6% respectively.

* **Job 3: 13-Month Sales Performance & MoM Growth**  
  Tracks revenue and calculates month-over-month (MoM) growth velocity.  
  *Finding:* Peak revenue occurred in December 2025 (154,486.05 AED Net across 31 orders), followed by an expected seasonal post-holiday decline in January and February 2026.

---

## 5. Repository Structure

```
├── process_datalake.py         # Production PySpark pipeline (Stage 3 & 4 execution)
├── pipeline_spark_processing.py # Standalone Python processing pipeline script
├── schema.hql                  # External Hive Parquet table definitions
├── Hadoop_Structure.sh         # HDFS directory structure setup script
├── README.md                   # Project documentation and execution instructions
└── .gitignore                  # Git ignore rules for datasets and artifacts
```

---

## 6. Execution Instructions

### Prerequisites
* Hadoop 2.7+ (HDFS, YARN)
* Apache Spark 3.0+
* Apache Hive 2.1+

### Step 1: Initialize Hadoop Daemons & HDFS Directories
```bash
$HADOOP_HOME/sbin/hadoop-daemon.sh start namenode
$HADOOP_HOME/sbin/hadoop-daemon.sh start datanode
$HADOOP_HOME/sbin/yarn-daemon.sh start resourcemanager
$HADOOP_HOME/sbin/yarn-daemon.sh start nodemanager

# Run directory setup
bash Hadoop_Structure.sh
```

### Step 2: Register Curated Parquet Schemas in Hive
```bash
hive -f schema.hql
```

### Step 3: Run the PySpark Processing Pipeline
```bash
export PYSPARK_PYTHON=python3
spark-submit --master local[*] process_datalake.py
```

### Step 4: Verify Tables via Hive CLI
```bash
hive -e "SELECT product_id, sku, product_name, regular_price, stock_status FROM dim_product LIMIT 5;"
hive -e "SELECT year_month, items_sold, total_orders, gross_sales, net_sales FROM fact_sales_monthly LIMIT 5;"
```

---

## 7. Deliverables Handoff to Group Members

All outputs generated by this pipeline are synchronized with the team:
* **Member 5 (Anusha)**: Shared HDFS raw paths and Hive schema coordination.
* **Member 7 (Paraag)**: Integrated pipeline code, execution logs, and performance metrics into the consolidated project report and presentation deck.
