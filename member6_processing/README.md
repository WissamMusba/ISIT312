# Member 6: Data Processing Pipeline and Sample Analytical Jobs (Coding Part 2)

**Contributor**: Wissam Musba (Member 6, Student ID: 8973465)  
**Project**: ISIT312 Big Data Management — Techno Store Middle East LLC  
**Role Scope**: Automated Apache Spark processing pipeline, Stage 3 data cleansing (HTML entity decoding, surrogate SKU imputation, price currency standardization, AOV recalculation), Curated Parquet Lakehouse generation, Stage 4 analytical jobs, and Hive schema-on-read query verification.

---

## 1. Overview of Member 6 Responsibilities

Member 6 owns the **Silver & Gold Layers (Processing, Quality & Analytics)** of the data lake:
1. **Bronze-to-Silver Ingestion & Cleansing (Stage 3)**: Reading raw CSVs ingested by Member 5 from HDFS `/datalake/raw/`, resolving severe raw data quality defects, and writing cleaned Silver datasets.
2. **Silver-to-Gold Parquet Curation**: Generating optimized, compressed columnar Parquet files and publishing them directly into the HDFS paths defined by Member 5's external Hive tables (`/datalake/curated/runs/run_001/*`).
3. **Stage 4 Sample Analytical Jobs**: Writing and executing business intelligence jobs in Apache Spark to extract actionable commercial insights for Techno Store management.
4. **Hive Schema-on-Read Verification**: Querying the curated Parquet datasets via Hive CLI / Beeline to confirm external table accessibility.
5. **Integration Handoff to Member 7 (Paraag)**: Providing execution logs, runtimes, cleaning yield metrics, and genuine terminal screenshots for final report assembly.

---

## 2. Stage 3 Data Quality Issues & Transformations

The raw WooCommerce export contained several structural quality defects that required automated remediation:

| Data Quality Issue | Raw Defect Description | Remediation Logic in PySpark | Yield / Impact |
| :--- | :--- | :--- | :--- |
| **HTML Entity Encoding** | 519 product names and brand strings contained HTML entities like `&amp;` and `&#039;` | Python `html.unescape()` UDF unescaping encoded strings to standard UTF-8 characters | Preserved brand integrity (e.g., prevented "Allen & Heath" from being split) |
| **Missing Primary Keys** | 25 product catalog items had blank / null SKU values | Synthesized unique surrogate keys: `concat(lit("SKU-AUTO-"), col("ID"))` | 100% record retention (5,929 rows) without breaking relational joins |
| **Price Data Standardization** | Inconsistent price strings, formatting artifacts, and blank values | Cast to `DoubleType()`, filled missing values cleanly with `0.0` AED | Prevented runtime null-pointer exceptions in arithmetic aggregations |
| **Category Hierarchy Flattening** | Deep nested taxonomy strings (e.g. `Audio > Mixers > Digital`) | Extracted root categories and sanitized full pathways | Clean grouping across 398 category paths |
| **Duplicated Sales Metric** | Static repeated average order value exported across all rows in WooCommerce | Divided net sales by order count per month to compute true monthly AOV | Replaced inaccurate flat static metric with actual dynamic monthly values |

---

## 3. Stage 4 Analytical Jobs & Business Insights

Member 6 designed and executed three commercial analytical jobs using PySpark DataFrame API and Spark SQL:

### Job 1: Brand Volume and Price Analysis
* **Objective**: Evaluate brand concentration, stock distribution, and pricing profiles across inventory.
* **Findings**:
  * **Top Volume Brands**: Yamaha (418 products), RCF (268 products), and Tannoy (251 products) account for the majority of catalog volume.
  * **Highest Value Brand**: Soundcraft represents high-end professional studio equipment with the highest average catalog price of **25,480 AED**.

### Job 2: Product Price Tier Distribution
* **Objective**: Segment 5,929 catalog items into commercial price bands to profile store positioning.
* **Findings**:
  * **Mid-Range Dominance**: **51.1%** of inventory (3,032 products) falls into the Mid-Range tier (1,000 to 5,000 AED).
  * **Professional Grade**: **24.2%** (5,000 to 15,000 AED).
  * **High-End Studio**: **12.6%** (>15,000 AED).
  * **Budget Gear**: **11.3%** (<1,000 AED).

### Job 3: 13-Month Sales Trend Analysis
* **Objective**: Analyze monthly revenue, order volume, and seasonal seasonality from May 2025 to May 2026.
* **Findings**:
  * **Peak Revenue**: December 2025 achieved record net sales of **154,486 AED** across 31 orders (holiday and year-end demand surge).
  * **Post-Holiday Trough**: Revenue contracted normally in Q1 (-20.4% in February, -19.5% in March 2026).

---

## 4. Script Catalog & Execution Guide

### `process_datalake.py`
The primary PySpark execution script running on Apache Spark 3.0.3:
```bash
spark-submit \
  --master local[*] \
  --deploy-mode client \
  process_datalake.py
```

### `schema.hql`
Automated Hive DDL and schema-on-read query verification script:
```bash
hive -f schema.hql
```
Verifies table counts and runs verification queries across `dim_product`, `dim_brand`, `dim_category`, `dim_date`, and `fact_sales_monthly`.

### `pipeline_spark_processing.py`
Modular production pipeline script incorporating robust error handling, metric checkpoints, and clean logging.
