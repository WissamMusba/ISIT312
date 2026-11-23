# Member 5: Data Ingestion and Storage Implementation (Coding Part 1)

**Contributor**: Anusha (Member 5)  
**Project**: ISIT312 Big Data Management — Techno Store Middle East LLC  
**Role Scope**: HDFS lakehouse directory hierarchy setup, raw CSV dataset ingestion, Hive metastore service initialization, and external Star Schema DDL definition.

---

## 1. Overview of Member 5 Responsibilities

Member 5 owns the **Bronze Layer (Ingestion & Storage)** of the data lake:
1. **Cluster Service Initialization**: Starting essential Hadoop ecosystem daemons including NameNode, DataNode, YARN ResourceManager, NodeManager, JobHistoryServer, Hive Metastore, and HiveServer2.
2. **HDFS Lakehouse Storage Topology**: Creating standardized directory hierarchies partitioned by ingestion date (`/datalake/raw/`, `/datalake/cleansed/`, `/datalake/curated/runs/run_001/`, `/datalake/_meta`).
3. **Raw Dataset Ingestion**: Copying the raw WooCommerce product catalog export (5,929 items, 42.9 MB) and monthly sales summary CSV into partitioned HDFS raw folders.
4. **Hive Metastore Schema Definition**: Establishing the external star schema tables (`dim_product`, `dim_brand`, `dim_category`, `dim_date`, and `fact_sales_monthly`) pointing to the curated Parquet directory locations.

---

## 2. Execution Order & Script Catalog

All scripts are executed inside the Hadoop VirtualBox VM environment in the following strict order:

### Step 1: Start Hadoop Daemons & Create HDFS Hierarchy
Execute `Hadoop_Structure.sh` to start Hadoop services and provision the lakehouse directory structure:
```bash
bash Hadoop_Structure.sh
```
* **Daemons started**: `namenode`, `datanode`, `resourcemanager`, `nodemanager`, `historyserver`
* **HDFS Directories provisioned**:
  * Raw Bronze: `/datalake/raw/woocommerce/catalogue/ingest_date=YYYY-MM-DD/` and `.../sales/ingest_date=YYYY-MM-DD/`
  * Cleansed Silver: `/datalake/cleansed/catalogue/`, `/datalake/cleansed/sales/`, `/datalake/cleansed/_quarantine/`
  * Curated Gold: `/datalake/curated/runs/run_001/dim_product/`, `dim_brand/`, `dim_category/`, `dim_date/`, `fact_sales_monthly/`
  * Metadata: `/datalake/_meta/`

### Step 2: Ingest Raw Datasets into HDFS
Execute `InsertDatasets.sh` to copy raw datasets from local storage into HDFS:
```bash
bash InsertDatasets.sh
```
* Transfers `wc-product-export-28-5-2026-1779968644501.csv` into `/datalake/raw/woocommerce/catalogue/ingest_date=$(date +%Y-%m-%d)/`
* Transfers `report-custom-2026-05-28.csv` into `/datalake/raw/woocommerce/sales/ingest_date=$(date +%Y-%m-%d)/`
* Verifies with: `hadoop fs -ls -R /datalake/raw/`

### Step 3: Launch Zeppelin & Hive Metastore
Execute `StartZeppelin.sh` (leave running in background):
```bash
bash StartZeppelin.sh
```
* Starts Zeppelin daemon: `$ZEPPELIN_HOME/bin/zeppelin-daemon.sh start`
* Starts Hive Metastore service: `$HIVE_HOME/bin/hive --service metastore`

### Step 4: Launch HiveServer2
Execute `StartHive2.sh` (leave running in background):
```bash
bash StartHive2.sh
```
* Starts HiveServer2 daemon: `$HIVE_HOME/bin/hiveserver2`

### Step 5: Star Schema DDL Registration
Import `Schema_Creation.json` into Apache Zeppelin as a notebook note (or execute equivalent DDL in Hive CLI):
* Creates external Hive tables stored as Parquet over the curated paths:
  * `dim_product` -> `/datalake/curated/runs/run_001/dim_product`
  * `dim_brand` -> `/datalake/curated/runs/run_001/dim_brand`
  * `dim_category` -> `/datalake/curated/runs/run_001/dim_category`
  * `dim_date` -> `/datalake/curated/runs/run_001/dim_date`
  * `fact_sales_monthly` -> `/datalake/curated/runs/run_001/fact_sales_monthly`

---

## 3. Integration Handoff to Member 6

Once Member 5 finishes data ingestion and external schema creation:
- Raw datasets reside safely in `/datalake/raw/` in HDFS.
- External Hive tables point to `/datalake/curated/runs/run_001/*`.
- **Member 6 (Wissam Musba)** takes over to run **Coding Part 2 (Processing Pipeline & Analytics)**: cleaning data defects, writing Parquet data into the curated directories, and running Spark analytical jobs.
