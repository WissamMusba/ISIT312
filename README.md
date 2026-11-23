# ISIT312 Big Data Management — Techno Store Middle East LLC
## Enterprise Big Data Lakehouse Implementation & Analytics

[![Course](https://img.shields.io/badge/Course-ISIT312%20Big%20Data%20Management-blue.svg)](https://www.uow.edu.au)
[![Term](https://img.shields.io/badge/Term-Autumn%202026-brightgreen.svg)]()
[![Instructor](https://img.shields.io/badge/Instructor-Dr.%20Patrick%20Mukala-orange.svg)]()
[![Apache Hadoop](https://img.shields.io/badge/Hadoop-3.3.1-yellow.svg)](https://hadoop.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-3.0.3-red.svg)](https://spark.apache.org/)
[![Apache Hive](https://img.shields.io/badge/Hive-3.1.2-purple.svg)](https://hive.apache.org/)

This repository houses the full codebase, cluster orchestration scripts, data cleansing pipelines, and analytical jobs for the **Techno Store Middle East LLC** big data project.

---

## 1. Project Overview & Business Case

Techno Store Middle East LLC is a premier professional audio, studio recording, and musical equipment retailer in the UAE. The organization faced significant data scalability bottlenecks:
- **E-commerce Platform**: WooCommerce running on legacy relational storage with flat-file CSV dumps.
- **Data Volume**: 5,929 product catalog records, complex nested taxonomies, and 13 months of transactional sales reports.
- **The Challenge**: Slow ad-hoc queries, high memory utilization, unformatted multi-line CSVs, and widespread data quality defects (broken HTML entity strings, missing SKUs, unstandardized pricing).
- **The Solution**: An enterprise-grade, multi-zone Big Data Lakehouse built on the **Hadoop Ecosystem (HDFS, Apache Spark, and Apache Hive)** utilizing the **Medallion Architecture (Bronze -> Silver -> Gold)**.

---

## 2. 7-Member Team Work Distribution

The project is structured across seven specialized group member roles:

| Member | Student Name | Assigned Role | Deliverable Focus |
| :---: | :--- | :--- | :--- |
| **Member 1** | Iqra | Research & Motivation | Business background, retail pain points, commercial rationale, and stakeholders |
| **Member 2** | Shrreya | Existing Infrastructure | Current relational systems, 44MB volume profile, storage limits, and bottlenecks |
| **Member 3** | Saarah | Technical Requirements | Batch vs. streaming requirements, latency SLAs, and schema flattening requirements |
| **Member 4** | Snaha | Proposed Architecture | Multi-zone Medallion lakehouse design (Bronze/Silver/Gold) on Hadoop ecosystem |
| **Member 5** | **Anusha** | **Coding Part 1 (Ingestion & Storage)** | **HDFS lakehouse folders, raw CSV ingestion, Hive metastore & external DDL** |
| **Member 6** | **Wissam Musba** | **Coding Part 2 (Processing & Analytics)** | **Spark cleansing pipeline, Parquet generation, 3 analytical jobs, Hive verification** |
| **Member 7** | Paraag | Evaluation & Group Coordination | Benchmark validation, scalability review, and consolidated final report & deck |

> **Note on Coding Deliverables**: Only **Member 5** and **Member 6** produce technical codebase assets. Member 5 builds the raw ingestion and Hive schema foundations; Member 6 implements the Spark cleansing, Parquet conversion, and analytical intelligence.

---

## 3. Data Lakehouse Architecture & Clean Separation

To ensure that components do not get mixed up, the technical implementation is divided into two distinct, decoupled directories:

```
ISIT312/
├── README.md                           # Main architecture documentation and pipeline guide
├── .gitignore                          # Standard git ignore for big data environments
│
├── member5_ingestion/                  # Coding Part 1 (Anusha - Ingestion & Storage)
│   ├── README.md                       # Member 5 guide and step-by-step execution instructions
│   ├── Hadoop_Structure.sh             # Starts Hadoop daemons and provisions HDFS lakehouse topology
│   ├── InsertDatasets.sh               # Ingests raw WooCommerce & Sales CSVs into partitioned HDFS raw folders
│   ├── StartZeppelin.sh                # Starts Zeppelin daemon and Hive metastore service
│   ├── StartHive2.sh                   # Starts HiveServer2 daemon
│   └── Schema_Creation.json            # Zeppelin notebook / Hive DDL defining external Parquet Star Schema
│
└── member6_processing/                 # Coding Part 2 (Wissam Musba - Processing & Analytics)
    ├── README.md                       # Member 6 guide, data cleansing yield, and analytical findings
    ├── process_datalake.py             # Complete PySpark pipeline (Stage 3 Cleansing & Stage 4 Analytics)
    ├── pipeline_spark_processing.py    # Modular production Spark batch processing pipeline
    └── schema.hql                      # Hive DDL external registration and live SQL verification queries
```

### Decoupled Hand-Off Workflow:
```
[Raw CSVs] 
    │
    ▼ (Member 5: InsertDatasets.sh)
[HDFS Bronze Zone: /datalake/raw/]
    │
    ▼ (Member 6: process_datalake.py - Stage 3 Cleansing)
[HDFS Silver Zone: /datalake/cleansed/] (Clean brands, imputed SKUs, AED prices)
    │
    ▼ (Member 6: process_datalake.py - Parquet Writer)
[HDFS Gold Zone: /datalake/curated/runs/run_001/] (Optimized Columnar Parquet)
    │
    ├────────────────────────────────────────┐
    ▼                                        ▼
[Member 5 & 6: Hive Metastore]         [Member 6: Stage 4 Spark Analytics]
(dim_product, dim_brand, dim_category,    Job 1: Brand Volume & Pricing
 dim_date, fact_sales_monthly)            Job 2: Price Tier Distribution
                                          Job 3: 13-Month Sales Trends
```

---

## 4. End-to-End Execution Guide

### Prerequisites
- VirtualBox VM: `BigDataVM-2021v2_2` (Ubuntu 64-bit, 4 vCPUs, 8 GB RAM)
- Hadoop 3.3.1, Spark 3.0.3 (PySpark), Hive 3.1.2, Java 8/11

### Step 1: Execute Member 5 Ingestion Scripts
```bash
# In the VM terminal:
cd /home/bigdata/ISIT312/member5_ingestion

# Start Hadoop daemons and create HDFS directories
bash Hadoop_Structure.sh

# Copy raw CSVs into HDFS Bronze raw folders
bash InsertDatasets.sh

# Start Metastore and HiveServer2
bash StartZeppelin.sh
bash StartHive2.sh
```

### Step 2: Execute Member 6 Processing & Analytics Pipeline
```bash
cd /home/bigdata/ISIT312/member6_processing

# Run the complete Spark Data Lakehouse Pipeline
spark-submit \
  --master local[*] \
  --deploy-mode client \
  process_datalake.py
```

### Step 3: Verify Schema-on-Read Queries in Apache Hive
```bash
# Execute Hive verification script
hive -f schema.hql
```

Sample verification output:
```sql
SELECT brand_name, count(*) as count 
FROM dim_brand 
JOIN dim_product ON dim_brand.brand_id = dim_product.brand_id 
GROUP BY brand_name ORDER BY count DESC LIMIT 5;

-- Result:
-- Yamaha       418
-- RCF          268
-- Tannoy       251
-- JBL          189
-- Shure        162
```

---

## 5. Summary of Commercial Insights Delivered

1. **Brand Dominance**: Yamaha, RCF, and Tannoy represent the core volume drivers. Soundcraft commands the highest average unit price (25,480 AED) as specialized studio equipment.
2. **Product Price Distribution**: Over 51% of products lie in the mid-range tier (1,000 to 5,000 AED), showing Techno Store is strongly anchored in serious enthusiast and commercial retail.
3. **Seasonality Performance**: December 2025 achieved record revenue (154,486 AED across 31 orders), followed by expected Q1 post-holiday reductions (-20.4% in February, -19.5% in March 2026).

---

## 6. Academic Integrity & Deliverables

- **Student**: Wissam Musba (ID: 8973465)
- **Role**: Member 6 (Coding Part 2: Processing Pipeline & Sample Jobs)
- **Repository Link**: [https://github.com/WissamMusba/ISIT312](https://github.com/WissamMusba/ISIT312)
