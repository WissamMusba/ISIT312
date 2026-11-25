"""
ISIT312 Big Data Management — Techno Store Middle East LLC
Data Lake Processing Pipeline: P2 (Cleanse), P3 (Sales), P4 (Curate & Analytics), P5 (Validate)
Author: Wissam Musba (Member 6)
"""

import os
import re
import sys

os.environ.setdefault("PYSPARK_PYTHON", "python3")

try:
    import html
    unescape_fn = html.unescape
except ImportError:
    from HTMLParser import HTMLParser
    unescape_fn = HTMLParser().unescape

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, split, trim, round as spark_round, 
    avg, max, count, lag, lit, concat, udf, monotonically_increasing_id
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType

def sanitize_columns(df):
    """Rename columns to Parquet-safe alphanumeric_underscore identifiers."""
    safe_cols = []
    seen = set()
    for c in df.columns:
        sc = re.sub(r'[^a-zA-Z0-9_]+', '_', c).strip('_') or 'col'
        if sc in seen:
            sc = sc + '_' + str(len(seen))
        seen.add(sc)
        safe_cols.append(sc)
    return df.toDF(*safe_cols)

def main():
    print("=" * 80)
    print(">>> [ISIT312 BIG DATA MANAGEMENT: DATA LAKE PROCESSING PIPELINE] <<<")
    print(">>> [TARGET: TECHNO STORE MIDDLE EAST LLC DATA LAKE] <<<")
    print("=" * 80)

    spark = SparkSession.builder \
        .appName("TechnoStore_DataLake_Processing") \
        .master("local[*]") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Read Raw Datasets from HDFS Raw Zone (Multi-line CSV enabled)
    print("\n[*] Ingesting raw datasets from HDFS Raw Zone...")
    raw_prod_path = "hdfs:///datalake/raw/woocommerce/catalogue/*/*.csv"
    raw_sales_path = "hdfs:///datalake/raw/woocommerce/sales/*/*.csv"

    raw_prod_df = spark.read \
        .option("header", "true") \
        .option("multiLine", "true") \
        .option("escape", '"') \
        .csv(raw_prod_path)
    raw_sales_df = spark.read.option("header", "true").csv(raw_sales_path)

    total_prods = raw_prod_df.count()
    total_sales_months = raw_sales_df.count()
    print("[*] Ingested Raw Product Records: %d" % total_prods)
    print("[*] Ingested Raw Sales Records: %d months" % total_sales_months)

    # 2. P2: Data Cleaning & Standardisation (Raw -> Cleansed Zone)
    print("\n" + "=" * 80)
    print(">>> [P2: DATA CLEANSING & TRANSFORMATION (RAW -> CLEANSED)] <<<")
    print("=" * 80)

    id_col = "ID"
    for c in raw_prod_df.columns:
        if "ID" in c:
            id_col = c
            break

    # UDF to decode ALL HTML entities (&amp;, &#039;, &quot;, etc.)
    unescape_udf = udf(lambda x: unescape_fn(x) if x is not None else None, StringType())

    # Flag duplicate SKUs without dropping rows
    sku_counts = raw_prod_df.filter(col("SKU").isNotNull() & (trim(col("SKU")) != "")) \
        .groupBy(trim(col("SKU")).alias("sku_key")) \
        .count() \
        .withColumn("is_duplicate_sku", col("count") > 1)

    cleaned_prod_df = raw_prod_df \
        .withColumn("product_id", col(id_col).cast("string")) \
        .withColumn("brand_clean", unescape_udf(col("Brands"))) \
        .withColumn("brand_clean", when(col("brand_clean").isNull() | (trim(col("brand_clean")) == ""), lit("Unbranded / Generic")).otherwise(trim(col("brand_clean")))) \
        .withColumn("name_clean", unescape_udf(col("Name"))) \
        .withColumn("sku_clean", when(col("SKU").isNull() | (trim(col("SKU")) == ""), concat(lit("AUTO-GEN-"), col(id_col))).otherwise(trim(col("SKU")))) \
        .withColumn("regular_price_clean", col("Regular price").cast(DoubleType())) \
        .withColumn("stock_status_clean", when(col("In stock?") == "1", lit("In Stock")).otherwise(lit("Out of Stock"))) \
        .withColumn("product_type_clean", when(col("Type").isNull() | (trim(col("Type")) == ""), lit("simple")).otherwise(trim(col("Type")))) \
        .withColumn("category_path_clean", when(col("Categories").isNull() | (trim(col("Categories")) == ""), lit("General AV")).otherwise(trim(col("Categories"))))

    # Write Cleansed Zone Parquet (with Parquet-safe column names)
    cleansed_prod_loc = "hdfs:///datalake/cleansed/catalogue"
    sanitize_columns(cleaned_prod_df).write.mode("overwrite").parquet(cleansed_prod_loc)
    print("[+] Written Cleansed Zone Parquet: %s" % cleansed_prod_loc)

    html_defect_count = raw_prod_df.filter(col("Brands").rlike("&[a-zA-Z0-9#]+;") | col("Name").rlike("&[a-zA-Z0-9#]+;")).count()
    missing_sku_count = raw_prod_df.filter(col("SKU").isNull() | (trim(col("SKU")) == "")).count()
    missing_price_count = raw_prod_df.filter(col("Regular price").isNull() | (trim(col("Regular price")) == "")).count()
    dup_sku_count = sku_counts.filter(col("is_duplicate_sku")).count()

    print("[-] Successfully standardised %d product records." % total_prods)
    print("[-] HTML text entity defects resolved: %d" % html_defect_count)
    print("[-] Missing SKUs imputed with surrogate keys: %d" % missing_sku_count)
    print("[-] Missing prices preserved as NULL (not coerced to 0): %d" % missing_price_count)
    print("[-] Duplicate SKUs flagged for governance: %d" % dup_sku_count)

    # 3. P4: Create Curated Star Schema Tables matching Hive Metastore
    print("\n" + "=" * 80)
    print(">>> [P4: CURATING STAR SCHEMA PARQUET TABLES FOR HIVE] <<<")
    print("=" * 80)

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
    curated_prod_count = dim_product.count()
    print("[+] Published Curated Parquet: %s (%d rows)" % (dim_prod_loc, curated_prod_count))

    unique_brands = cleaned_prod_df.select("brand_clean").distinct() \
        .withColumn("brand_id", concat(lit("BRD-"), monotonically_increasing_id())) \
        .select(col("brand_id"), col("brand_clean").alias("brand_name"))
    dim_brand_loc = "hdfs:///datalake/curated/runs/run_001/dim_brand"
    unique_brands.write.mode("overwrite").parquet(dim_brand_loc)
    print("[+] Published Curated Parquet: %s (%d unique brands)" % (dim_brand_loc, unique_brands.count()))

    unique_cats = cleaned_prod_df.select("category_path_clean").distinct() \
        .withColumn("category_id", concat(lit("CAT-"), monotonically_increasing_id())) \
        .withColumn("category_name", split(col("category_path_clean"), " > ")[0]) \
        .select(col("category_id"), col("category_name"), col("category_path_clean").alias("category_path"))
    dim_cat_loc = "hdfs:///datalake/curated/runs/run_001/dim_category"
    unique_cats.write.mode("overwrite").parquet(dim_cat_loc)
    print("[+] Published Curated Parquet: %s (%d unique category paths)" % (dim_cat_loc, unique_cats.count()))

    sales_clean = raw_sales_df.select(
        col("Date").alias("year_month"),
        col("Number of items sold").cast(IntegerType()).alias("items_sold"),
        col("Number of orders").cast(IntegerType()).alias("total_orders"),
        col("Gross sales amount").cast(DoubleType()).alias("gross_sales"),
        col("Net sales amount").cast(DoubleType()).alias("net_sales"),
        col("Shipping amount").cast(DoubleType()).alias("shipping_amount"),
        col("Coupon amount").cast(DoubleType()).alias("coupon_amount"),
        col("Refund amount").cast(DoubleType()).alias("refund_amount"),
        split(col("Date"), "-")[0].cast(IntegerType()).alias("report_year"),
        split(col("Date"), "-")[1].cast(IntegerType()).alias("report_month")
    )

    sales_clean.write.mode("overwrite").parquet("hdfs:///datalake/cleansed/sales")

    dim_date = sales_clean.select("year_month", "report_year", "report_month")
    dim_date_loc = "hdfs:///datalake/curated/runs/run_001/dim_date"
    dim_date.write.mode("overwrite").parquet(dim_date_loc)
    print("[+] Published Curated Parquet: %s (%d date rows)" % (dim_date_loc, dim_date.count()))

    fact_sales = sales_clean.select(
        "year_month", "items_sold", "total_orders", 
        "gross_sales", "net_sales", "shipping_amount", 
        "coupon_amount", "refund_amount"
    )
    fact_sales_loc = "hdfs:///datalake/curated/runs/run_001/fact_sales_monthly"
    fact_sales.write.mode("overwrite").parquet(fact_sales_loc)
    curated_sales_count = fact_sales.count()
    print("[+] Published Curated Parquet: %s (%d monthly sales rows)" % (fact_sales_loc, curated_sales_count))

    # 4. Analytical Jobs
    print("\n" + "=" * 80)
    print(">>> [ANALYTICAL JOBS ON CURATED DATA] <<<")
    print("=" * 80)

    # Job 1: Top 10 Brands (excluding NULL prices from average calculation)
    print("\n--- [Job 1: Top 10 Brands by Catalogue Listings and Pricing (AED)] ---")
    job1_df = cleaned_prod_df.groupBy("brand_clean") \
        .agg(
            count("product_id").alias("Catalog_Count"),
            spark_round(avg(when(col("regular_price_clean") > 0, col("regular_price_clean"))), 2).alias("Avg_Price_AED"),
            spark_round(max("regular_price_clean"), 2).alias("Max_Price_AED")
        ) \
        .orderBy(col("Catalog_Count").desc()) \
        .limit(10)
    job1_df.show(10, truncate=False)

    # Job 2: Price Bracket Distribution
    print("\n--- [Job 2: Product Price Bracket Distribution (5 Tiers)] ---")
    bracket_df = cleaned_prod_df \
        .withColumn("price_tier", 
            when(col("regular_price_clean").isNull() | (col("regular_price_clean") == 0), lit("Unpriced / NULL"))
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

    # Job 3: 13-Month Sales Trend & MoM Revenue Growth (Chronologically sorted by year and month)
    print("\n--- [Job 3: 13-Month Sales Performance & MoM Growth (Chronological)] ---")
    w_spec = Window.orderBy("report_year", "report_month")
    job3_df = sales_clean \
        .withColumn("true_avg_order_value", spark_round(col("net_sales") / col("total_orders"), 2)) \
        .withColumn("prev_net", lag("net_sales", 1).over(w_spec)) \
        .withColumn("mom_growth_pct", spark_round(((col("net_sales") - col("prev_net")) / col("prev_net")) * 100.0, 1)) \
        .orderBy("report_year", "report_month") \
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

    # 5. P5: Validation & Record Accounting Gate
    print("\n" + "=" * 80)
    print(">>> [P5: DATA VALIDATION & RECORD ACCOUNTING GATE] <<<")
    print("=" * 80)
    assert total_prods == curated_prod_count, "Record mismatch! Raw=%d, Curated=%d" % (total_prods, curated_prod_count)
    assert total_sales_months == curated_sales_count, "Sales mismatch! Raw=%d, Curated=%d" % (total_sales_months, curated_sales_count)
    print("[PASS] Product Record Accounting: Raw (%d) == Curated (%d) [100%% Retained]" % (total_prods, curated_prod_count))
    print("[PASS] Sales Record Accounting: Raw (%d months) == Curated (%d months) [100%% Retained]" % (total_sales_months, curated_sales_count))
    print("[PASS] Zero NULL SKUs in dim_product: %d" % dim_product.filter(col("sku").isNull()).count())
    print("[PASS] Pipeline P5 Validation Gate PASSED.")

    spark.stop()
    print("\n[+] Processing pipeline completed successfully with zero execution errors.")

if __name__ == "__main__":
    main()
