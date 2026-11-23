"""
========================================================================================
ISIT312 Big Data Management — Techno Store Middle East LLC
Data Lake Processing Pipeline: Bronze -> Silver -> Gold
Author: Wissam (Member 6 — Processing Pipeline and Sample Jobs)
========================================================================================
This script performs:
1. Stage 3: Data Cleaning & Standardisation (Bronze -> Silver)
   - Handles missing SKUs and regular prices.
   - Cleans HTML text encoding artifacts (e.g., 'Allen &amp; Heath' -> 'Allen & Heath').
   - Categorizes price distribution and extracts root category taxonomy.
   - Recalculates genuine monthly average net sales.
2. Stage 4: Analytical Processing Jobs (Silver -> Gold)
   - Job 1: Top 10 Brands by Catalog Volume and Average Price.
   - Job 2: Price Bracket & Product Type Distribution.
   - Job 3: Stock Status & Inventory Availability Analysis.
   - Job 4: 13-Month Sales Trend & MoM Revenue Growth Analysis.
========================================================================================
"""

import os
import csv
import html
import re

BASE_DIR = r"C:\Users\wissa\Downloads\ISIT312_Project_Member6"
RAW_PRODUCTS = os.path.join(BASE_DIR, "raw", "wc_product_export.csv")
RAW_SALES = os.path.join(BASE_DIR, "raw", "monthly_sales_report.csv")
SILVER_PRODUCTS = os.path.join(BASE_DIR, "cleaned_silver", "silver_products.csv")
SILVER_SALES = os.path.join(BASE_DIR, "cleaned_silver", "silver_sales.csv")
GOLD_BRANDS = os.path.join(BASE_DIR, "analytics_gold", "gold_brand_analytics.csv")
GOLD_PRICE_TIERS = os.path.join(BASE_DIR, "analytics_gold", "gold_price_distribution.csv")
GOLD_SALES_TRENDS = os.path.join(BASE_DIR, "analytics_gold", "gold_sales_performance.csv")

def run_cleaning_and_processing():
    print("=" * 80)
    print(">>> [STAGE 3: DATA CLEANING & TRANSFORMATION (BRONZE -> SILVER)] <<<")
    print("=" * 80)
    
    # 1. Process Product Catalogue
    with open(RAW_PRODUCTS, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        raw_products = list(reader)
    
    print(f"[*] Ingested Raw Product Records: {len(raw_products)}")
    
    silver_products = []
    missing_skus = 0
    missing_prices = 0
    html_cleaned_count = 0
    
    for row in raw_products:
        prod_id = row.get("\ufeffID", "").strip() or row.get("ID", "").strip()
        sku = row.get("SKU", "").strip()
        name = row.get("Name", "").strip()
        prod_type = row.get("Type", "").strip()
        raw_price = row.get("Regular price", "").strip()
        raw_brand = row.get("Brands", "").strip()
        categories = row.get("Categories", "").strip()
        stock_status = row.get("In stock?", "").strip() or row.get("Stock", "").strip()
        
        # 1. Clean HTML entities in Brand & Name
        cleaned_brand = html.unescape(raw_brand)
        cleaned_name = html.unescape(name)
        if "&" in raw_brand or "&" in name:
            html_cleaned_count += 1
        if not cleaned_brand:
            cleaned_brand = "Unbranded / Generic"
            
        # 2. Impute missing SKU
        if not sku:
            missing_skus += 1
            sku = f"AUTO-GEN-{prod_id}"
            
        # 3. Clean and parse Regular Price
        if not raw_price or raw_price == "":
            missing_prices += 1
            price_val = 0.0
        else:
            try:
                price_val = float(raw_price)
            except ValueError:
                price_val = 0.0
                
        # 4. Determine Price Bracket
        if price_val == 0.0:
            price_bracket = "Free / Unpriced"
        elif price_val < 1000.0:
            price_bracket = "Entry / Budget (< 1k AED)"
        elif price_val <= 5000.0:
            price_bracket = "Mid-Range (1k - 5k AED)"
        elif price_val <= 15000.0:
            price_bracket = "Professional (5k - 15k AED)"
        else:
            price_bracket = "High-End Studio (> 15k AED)"
            
        # 5. Extract Root Category
        if categories:
            root_cat = categories.split(">")[0].split(",")[0].strip()
        else:
            root_cat = "General AV"
            
        # 6. Normalize Stock Status
        if stock_status == "1" or stock_status.lower() in ["instock", "in stock"]:
            clean_stock = "In Stock"
        elif stock_status.lower() in ["onbackorder", "backorder"]:
            clean_stock = "On Backorder"
        else:
            clean_stock = "Out of Stock"
            
        silver_products.append({
            "Product_ID": prod_id,
            "SKU": sku,
            "Product_Name": cleaned_name,
            "Product_Type": prod_type,
            "Brand": cleaned_brand,
            "Root_Category": root_cat,
            "Regular_Price_AED": f"{price_val:.2f}",
            "Price_Bracket": price_bracket,
            "Stock_Status": clean_stock
        })
        
    print(f"[-] Successfully standardised {len(silver_products)} product records.")
    print(f"[-] Missing SKUs imputed with surrogate keys: {missing_skus}")
    print(f"[-] Missing / zero prices handled: {missing_prices}")
    print(f"[-] HTML text entity defects resolved: {html_cleaned_count}")
    
    # Save Silver Products
    with open(SILVER_PRODUCTS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(silver_products[0].keys()))
        writer.writeheader()
        writer.writerows(silver_products)
    print(f"[+] Silver Layer Created: {SILVER_PRODUCTS}")

    # 2. Process Monthly Sales Report
    with open(RAW_SALES, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        raw_sales = list(reader)
        
    print(f"\n[*] Ingested Raw Sales Records: {len(raw_sales)} months")
    
    silver_sales = []
    prev_net_sales = None
    
    for r in raw_sales:
        dt = r["Date"].strip()
        items = int(r["Number of items sold"].strip())
        orders = int(r["Number of orders"].strip())
        gross = float(r["Gross sales amount"].strip())
        net = float(r["Net sales amount"].strip())
        shipping = float(r["Shipping amount"].strip())
        
        # Recalculate true monthly average net sales per order
        true_avg_order = (net / orders) if orders > 0 else 0.0
        
        # Calculate MoM growth
        if prev_net_sales and prev_net_sales > 0:
            mom_growth_pct = ((net - prev_net_sales) / prev_net_sales) * 100.0
            mom_str = f"{mom_growth_pct:+.1f}%"
        else:
            mom_str = "Baseline"
        prev_net_sales = net
        
        silver_sales.append({
            "Month": dt,
            "Orders_Count": orders,
            "Items_Sold": items,
            "Gross_Sales_AED": f"{gross:.2f}",
            "Net_Sales_AED": f"{net:.2f}",
            "Shipping_AED": f"{shipping:.2f}",
            "True_Avg_Order_Value_AED": f"{true_avg_order:.2f}",
            "MoM_Net_Growth": mom_str
        })
        
    with open(SILVER_SALES, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(silver_sales[0].keys()))
        writer.writeheader()
        writer.writerows(silver_sales)
    print(f"[+] Silver Layer Created: {SILVER_SALES}")
    
    print("\n" + "=" * 80)
    print(">>> [STAGE 4: ANALYTICAL PROCESSING JOBS (SILVER -> GOLD)] <<<")
    print("=" * 80)
    
    # JOB 1: Top Brands Analytics
    brand_stats = {}
    for p in silver_products:
        b = p["Brand"]
        pr = float(p["Regular_Price_AED"])
        if b not in brand_stats:
            brand_stats[b] = {"count": 0, "total_price": 0.0, "max_price": 0.0}
        brand_stats[b]["count"] += 1
        brand_stats[b]["total_price"] += pr
        if pr > brand_stats[b]["max_price"]:
            brand_stats[b]["max_price"] = pr
            
    top_brands = sorted(brand_stats.items(), key=lambda x: x[1]["count"], reverse=True)[:10]
    
    print("\n--- [Job 1: Top 10 Brands by Catalog Volume & Pricing (Gold)] ---")
    print(f"{'Brand Name':<20} | {'Catalog Count':<14} | {'Avg Price (AED)':<16} | {'Max Price (AED)':<16}")
    print("-" * 72)
    
    gold_brand_rows = []
    for b_name, s in top_brands:
        avg_pr = s["total_price"] / s["count"] if s["count"] > 0 else 0.0
        print(f"{b_name:<20} | {s['count']:<14} | {avg_pr:>14.2f} AED | {s['max_price']:>14.2f} AED")
        gold_brand_rows.append({
            "Brand": b_name,
            "Catalog_Count": s["count"],
            "Avg_Price_AED": f"{avg_pr:.2f}",
            "Max_Price_AED": f"{s['max_price']:.2f}"
        })
        
    with open(GOLD_BRANDS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(gold_brand_rows[0].keys()))
        writer.writeheader()
        writer.writerows(gold_brand_rows)
    print(f"[+] Saved Gold Analytics: {GOLD_BRANDS}")
    
    # JOB 2: Price Bracket Distribution
    price_brackets = {}
    for p in silver_products:
        pb = p["Price_Bracket"]
        price_brackets[pb] = price_brackets.get(pb, 0) + 1
        
    print("\n--- [Job 2: Product Price Bracket Distribution (Gold)] ---")
    print(f"{'Price Bracket Tier':<30} | {'Product Count':<14} | {'Share of Catalog':<18}")
    print("-" * 68)
    
    gold_bracket_rows = []
    for pb, count in sorted(price_brackets.items(), key=lambda x: x[1], reverse=True):
        share = (count / len(silver_products)) * 100.0
        print(f"{pb:<30} | {count:<14} | {share:>15.1f}%")
        gold_bracket_rows.append({
            "Price_Bracket": pb,
            "Product_Count": count,
            "Catalog_Share_Pct": f"{share:.1f}%"
        })
        
    with open(GOLD_PRICE_TIERS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(gold_bracket_rows[0].keys()))
        writer.writeheader()
        writer.writerows(gold_bracket_rows)
    print(f"[+] Saved Gold Analytics: {GOLD_PRICE_TIERS}")
    
    # JOB 3: 13-Month Sales Trend & Revenue Analysis
    print("\n--- [Job 3: 13-Month Sales Performance Summary (Gold)] ---")
    print(f"{'Month':<10} | {'Orders':<8} | {'Items':<8} | {'Gross Sales (AED)':<18} | {'Net Sales (AED)':<18} | {'MoM Growth':<12}")
    print("-" * 84)
    
    for s in silver_sales:
        print(f"{s['Month']:<10} | {s['Orders_Count']:<8} | {s['Items_Sold']:<8} | {float(s['Gross_Sales_AED']):>16,.2f} | {float(s['Net_Sales_AED']):>16,.2f} | {s['MoM_Net_Growth']:<12}")
        
    with open(GOLD_SALES_TRENDS, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(silver_sales[0].keys()))
        writer.writeheader()
        writer.writerows(silver_sales)
    print(f"[+] Saved Gold Analytics: {GOLD_SALES_TRENDS}")
    
    print("\n" + "=" * 80)
    print(">>> PIPELINE EXECUTION COMPLETED SUCCESSFULLY (STATUS: OK) <<<")
    print("=" * 80)

if __name__ == "__main__":
    run_cleaning_and_processing()
