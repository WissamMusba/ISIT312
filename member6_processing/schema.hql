CREATE EXTERNAL TABLE IF NOT EXISTS dim_product (
    product_id STRING,
    sku STRING,
    product_name STRING,
    product_type STRING,
    regular_price DOUBLE,
    stock_status STRING
)
STORED AS PARQUET
LOCATION '/datalake/curated/runs/run_001/dim_product';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_brand (
    brand_id STRING,
    brand_name STRING
)
STORED AS PARQUET
LOCATION '/datalake/curated/runs/run_001/dim_brand';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_category (
    category_id STRING,
    category_name STRING,
    category_path STRING
)
STORED AS PARQUET
LOCATION '/datalake/curated/runs/run_001/dim_category';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
    year_month STRING,
    report_year INT,
    report_month INT
)
STORED AS PARQUET
LOCATION '/datalake/curated/runs/run_001/dim_date';

CREATE EXTERNAL TABLE IF NOT EXISTS fact_sales_monthly (
    year_month STRING,
    items_sold INT,
    total_orders INT,
    gross_sales DOUBLE,
    net_sales DOUBLE,
    shipping_amount DOUBLE,
    coupon_amount DOUBLE,
    refund_amount DOUBLE
)
STORED AS PARQUET
LOCATION '/datalake/curated/runs/run_001/fact_sales_monthly';

SHOW TABLES;
