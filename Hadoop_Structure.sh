#!/bin/bash
# ISIT312 Big Data Management — Techno Store Middle East LLC
# HDFS Directory Layout Setup

echo ">>> Creating HDFS Directory Hierarchy <<<"
hadoop fs -mkdir -p /datalake/raw/woocommerce/catalogue/ingest_date=$(date +%Y-%m-%d)
hadoop fs -mkdir -p /datalake/raw/woocommerce/sales/ingest_date=$(date +%Y-%m-%d)
hadoop fs -mkdir -p /datalake/cleansed/catalogue
hadoop fs -mkdir -p /datalake/cleansed/sales
hadoop fs -mkdir -p /datalake/cleansed/_quarantine
hadoop fs -mkdir -p /datalake/curated/runs/run_001/dim_product
hadoop fs -mkdir -p /datalake/curated/runs/run_001/dim_brand
hadoop fs -mkdir -p /datalake/curated/runs/run_001/dim_category
hadoop fs -mkdir -p /datalake/curated/runs/run_001/dim_date
hadoop fs -mkdir -p /datalake/curated/runs/run_001/fact_sales_monthly
hadoop fs -mkdir -p /datalake/_meta

echo ">>> HDFS Directories Created Successfully <<<"
hadoop fs -ls -R /datalake/
