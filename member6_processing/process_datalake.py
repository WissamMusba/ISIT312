"""
ISIT312 Big Data Management — Techno Store Middle East LLC
Data Lake Processing Pipeline: Stage 3 (Cleanse) & Stage 4 (Analytics)
Author: Wissam (Member 6)
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, regexp_replace, split, trim, round as spark_round, 
    avg, max, count, lag, lit, concat, monotonically_increasing_id
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

def main():
    print("=" * 80)
    print(">>> [ISIT312 BIG DATA MANAGEMENT: MEMBER 6 PROCESSING PIPELINE] <<<")
    print(">>> [TARGET: TECHNO STORE MIDDLE EAST LLC DATA LAKE] <<<")
    print("=" * 80)

    spark = SparkSession.builder \
        .appName("TechnoStore_DataLake_Processing") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Read Raw Datasets from HDFS Raw Zone
    print("\n[*] Ingesting raw datasets from HDFS Raw Zone...")
    raw_prod_path = "hdfs:///datalake/raw/woocommerce/catalogue/ingest_date=2026-11-23/*.csv"
    raw_sales_path = "hdfs:///datalake/raw/woocommerce/sales/ingest_date=2026-11-23/*.csv"

    raw_prod_df = spark.read.option("header", "true").option("multiLine", "true").option("escape", '"').csv(raw_prod_path)
    raw_sales_df = spark.read.option("header", "true").csv(raw_sales_path)

    total_prods = raw_prod_df.count()
    total_sales_months = raw_sales_df.count()
    print("[*] Ingested Raw Product Records: %d" % total_prods)
    print("[*] Ingested Raw Sales Records: %d months" % total_sales_months)

    # 2. Stage 3: Data Cleaning & Standardisation
    print("\n" + "=" * 80)
    print(">>> [STAGE 3: DATA CLEANSING & TRANSFORMATION (RAW -> CLEANSED)] <<<")
    print("=" * 80)

    # Detect ID column
    id_col = "ID"
    for c in raw_prod_df.columns:
        if "ID" in c:
            id_col = c
            break

    # Clean HTML entities, impute missing SKUs, parse prices, clean stock
    cleaned_prod_df = raw_prod_df \
        .withColumn("product_id", col(id_col).cast("string")) \
        .withColumn("raw_brand", col("Brands")) \
        .withColumn("brand_clean", regexp_replace(col("Brands"), "&amp;", "&")) \
        .withColumn("brand_clean", when(col("brand_clean").isNull() | (trim(col("brand_clean")) == ""), lit("Unbranded / Generic")).otherwise(trim(col("brand_clean")))) \
        .withColumn("name_clean", regexp_replace(col("Name"), "&amp;", "&")) \
        .withColumn("sku_clean", when(col("SKU").isNull() | (trim(col("SKU")) == ""), concat(lit("AUTO-GEN-"), col(id_col))).otherwise(trim(col("SKU")))) \
        .withColumn("regular_price_clean", col("Regular price").cast(DoubleType())) \
        .fillna({"regular_price_clean": 0.0}) \
        .withColumn("stock_status_clean", when(col("In stock?") == "1", lit("In Stock")).otherwise(lit("Out of Stock"))) \
        .withColumn("product_type_clean", when(col("Type").isNull() | (trim(col("Type")) == ""), lit("simple")).otherwise(trim(col("Type")))) \
        .withColumn("category_path_clean", when(col("Categories").isNull() | (trim(col("Categories")) == ""), lit("General AV")).otherwise(trim(col("Categories"))))

    # Calculate cleaning metrics
    html_defect_count = raw_prod_df.filter(col("Brands").contains("&amp;") | col("Name").contains("&amp;")).count()
    missing_sku_count = raw_prod_df.filter(col("SKU").isNull() | (trim(col("SKU")) == "")).count()
    missing_price_count = raw_prod_df.filter(col("Regular price").isNull() | (trim(col("Regular price")) == "")).count()

    print("[-] Successfully standardised %d product records." % total_prods)
    print("[-] HTML text entity defects resolved: %d" % html_defect_count)
    print("[-] Missing SKUs imputed with surrogate keys: %d" % missing_sku_count)
    print("[-] Missing / zero prices handled: %d" % missing_price_count)

    # 3. Create Curated Star Schema Tables matching Hive Metastore
    print("\n" + "=" * 80)
    print(">>> [STAGE 4: CURATING STAR SCHEMA PARQUET TABLES FOR HIVE] <<<")
    print("=" * 80)

    # A. dim_product
    dim_product = cleaned_prod_df.select(
        col("product_id"),
        col("sku_clean").alias("sku"),
        col("name_clean").alias("product_name"),
        col("product_type_clean").alias("product_type"),
        col("regular_price_clean").alias("regular_price"),
        col("stock_status_clean").alias("stock_status")
    )
    dim_prod_loc = "hdfs:///datalake/curated/runs/run_001/dim_product"
    dim_product.write.mode("overwrite").parquet(dim_prod_loc)
    print("[+] Published Curated Parquet: %s (%d rows)" % (dim_prod_loc, dim_product.count()))

    # B. dim_brand
    unique_brands = cleaned_prod_df.select("brand_clean").distinct() \
        .withColumn("brand_id", concat(lit("BRD-"), monotonically_increasing_id())) \
        .select(col("brand_id"), col("brand_clean").alias("brand_name"))
    dim_brand_loc = "hdfs:///datalake/curated/runs/run_001/dim_brand"
    unique_brands.write.mode("overwrite").parquet(dim_brand_loc)
    print("[+] Published Curated Parquet: %s (%d unique brands)" % (dim_brand_loc, unique_brands.count()))

    # C. dim_category
    unique_cats = cleaned_prod_df.select("category_path_clean").distinct() \
        .withColumn("category_id", concat(lit("CAT-"), monotonically_increasing_id())) \
        .withColumn("category_name", split(col("category_path_clean"), " > ")[0]) \
        .select(col("category_id"), col("category_name"), col("category_path_clean").alias("category_path"))
    dim_cat_loc = "hdfs:///datalake/curated/runs/run_001/dim_category"
    unique_cats.write.mode("overwrite").parquet(dim_cat_loc)
    print("[+] Published Curated Parquet: %s (%d unique category paths)" % (dim_cat_loc, unique_cats.count()))

    # D. Process Sales Summary
    sales_clean = raw_sales_df \
        .withColumn("year_month", col("Date")) \
        .withColumn("items_sold", col("Number of items sold").cast(IntegerType())) \
        .withColumn("total_orders", col("Number of orders").cast(IntegerType())) \
        .withColumn("gross_sales", col("Gross sales amount").cast(DoubleType())) \
        .withColumn("net_sales", col("Net sales amount").cast(DoubleType())) \
        .withColumn("shipping_amount", col("Shipping amount").cast(DoubleType())) \
        .withColumn("coupon_amount", col("Coupon amount").cast(DoubleType())) \
        .withColumn("refund_amount", col("Refund amount").cast(DoubleType())) \
        .withColumn("report_year", split(col("Date"), "-")[0].cast(IntegerType())) \
        .withColumn("report_month", split(col("Date"), "-")[1].cast(IntegerType()))

    # E. dim_date
    dim_date = sales_clean.select("year_month", "report_year", "report_month")
    dim_date_loc = "hdfs:///datalake/curated/runs/run_001/dim_date"
    dim_date.write.mode("overwrite").parquet(dim_date_loc)
    print("[+] Published Curated Parquet: %s (%d date rows)" % (dim_date_loc, dim_date.count()))

    # F. fact_sales_monthly
    fact_sales = sales_clean.select(
        "year_month", "items_sold", "total_orders", 
        "gross_sales", "net_sales", "shipping_amount", 
        "coupon_amount", "refund_amount"
    )
    fact_sales_loc = "hdfs:///datalake/curated/runs/run_001/fact_sales_monthly"
    fact_sales.write.mode("overwrite").parquet(fact_sales_loc)
    print("[+] Published Curated Parquet: %s (%d monthly sales rows)" % (fact_sales_loc, fact_sales.count()))

    # 4. Stage 4: Run Sample Analytical Jobs
    print("\n" + "=" * 80)
    print(">>> [STAGE 4: ANALYTICAL JOBS ON CURATED DATA LAKE] <<<")
    print("=" * 80)

    # Job 1: Top 10 Brands by Catalog Volume and Pricing
    print("\n--- [Job 1: Top 10 Brands by Catalog Volume and Pricing (AED)] ---")
    job1_df = cleaned_prod_df.groupBy("brand_clean") \
        .agg(
            count("product_id").alias("Catalog_Count"),
            spark_round(avg("regular_price_clean"), 2).alias("Avg_Price_AED"),
            spark_round(max("regular_price_clean"), 2).alias("Max_Price_AED")
        ) \
        .orderBy(col("Catalog_Count").desc()) \
        .limit(10)
    job1_df.show(10, truncate=False)

    # Job 2: Price Bracket Distribution
    print("\n--- [Job 2: Product Price Bracket Distribution] ---")
    bracket_df = cleaned_prod_df \
        .withColumn("price_tier", 
            when(col("regular_price_clean") == 0, lit("Free / Unpriced"))
            .when(col("regular_price_clean") < 1000, lit("Entry / Budget (< 1k AED)"))
            .when(col("regular_price_clean") <= 5000, lit("Mid-Range (1k - 5k AED)"))
            .when(col("regular_price_clean") <= 15000, lit("Professional (5k - 15k AED)"))
            .otherwise(lit("High-End Studio (> 15k AED)"))
        )
    job2_df = bracket_df.groupBy("price_tier") \
        .agg(count("product_id").alias("Product_Count")) \
        .withColumn("Catalog_Share_Pct", spark_round((col("Product_Count") / lit(total_prods)) * 100.0, 1)) \
        .orderBy(col("Product_Count").desc())
    job2_df.show(truncate=False)

    # Job 3: 13-Month Sales Trend & MoM Revenue Growth Analysis
    print("\n--- [Job 3: 13-Month Sales Performance & MoM Growth] ---")
    w_spec = Window.orderBy("year_month")
    job3_df = fact_sales \
        .withColumn("true_avg_order_value", spark_round(col("net_sales") / col("total_orders"), 2)) \
        .withColumn("prev_net", lag("net_sales", 1).over(w_spec)) \
        .withColumn("mom_growth_pct", spark_round(((col("net_sales") - col("prev_net")) / col("prev_net")) * 100.0, 1)) \
        .select(
            col("year_month").alias("Month"),
            col("total_orders").alias("Orders"),
            col("items_sold").alias("Items"),
            col("gross_sales").alias("Gross_Sales_AED"),
            col("net_sales").alias("Net_Sales_AED"),
            col("true_avg_order_value").alias("True_AOV_AED"),
            col("mom_growth_pct").alias("MoM_Growth_Pct")
        )
    job3_df.show(15, truncate=False)

    spark.stop()
    print("\n[+] Processing pipeline completed successfully with zero execution errors.")

if __name__ == "__main__":
    main()
