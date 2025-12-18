import os
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from io import StringIO

# Placeholder URL - Replace with actual URL in production
DIRTY_LINE_ITEMS_URL = "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Test%20Files/dirty_line_item_data_products.csv"

def get_db_connection():
    return psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )

def main():
    print(f"Ingesting dirty line item data from {DIRTY_LINE_ITEMS_URL} into stg_line_item_data_products (Row-by-Row Mode)...")
    
    try:
        resp = requests.get(DIRTY_LINE_ITEMS_URL, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
    except Exception as e:
        print(f"Failed to fetch data from URL: {e}")
        return
    
    # ==========================================
    # 🧹 DATA CLEANING & VALIDATION
    # ==========================================

    # 1. Filter out rows with missing critical IDs
    missing_id_mask = df["Order_id"].isna() | (df["Order_id"] == "") | df["Product_id"].isna() | (df["Product_id"] == "")
    if missing_id_mask.any():
        dropped_count = missing_id_mask.sum()
        print(f"Warning: Dropping {dropped_count} rows with missing 'Order_id' or 'Product_id'.")
        df = df[~missing_id_mask]

    conn = get_db_connection()
    cur = conn.cursor()
    
    table_name = "stg_line_item_data_products"
    
    success_count = 0
    fail_count = 0
    
    for index, row in df.iterrows():
        try:
            insert_query = sql.SQL("""
                INSERT INTO {} (order_id, product_name, product_id)
                VALUES (%s, %s, %s)
            """).format(sql.Identifier(table_name))
            
            cur.execute(insert_query, (
                row['Order_id'], 
                row['Product_name'], 
                row['Product_id']
            ))
            conn.commit()
            success_count += 1
            
        except Exception as e:
            conn.rollback()
            fail_count += 1
            print(f"Row {index} FAILED: {row.to_dict()} -> Error: {e}")

    print(f"Finished. Success: {success_count}, Failed: {fail_count}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()