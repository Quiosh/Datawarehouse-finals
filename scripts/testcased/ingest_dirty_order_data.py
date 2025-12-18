import os
import requests
import pandas as pd
import psycopg2
from psycopg2 import sql
from io import StringIO

# Placeholder URL - Replace with actual URL in production
DIRTY_ORDER_DATA_URL = "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Test%20Files/dirty_order_data.csv"

def get_db_connection():
    return psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )

def main():
    print(f"Ingesting dirty order data from {DIRTY_ORDER_DATA_URL} into stg_order_data (Row-by-Row Mode)...")
    
    try:
        resp = requests.get(DIRTY_ORDER_DATA_URL, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(StringIO(resp.text))
    except Exception as e:
        print(f"Failed to fetch data from URL: {e}")
        return

    # ==========================================
    # 🧹 DATA CLEANING & VALIDATION
    # ==========================================
    
    # 1. Clean Estimated Arrival
    #    Remove non-digits, coerce to numeric, drop invalid
    df["Estimated_arrival"] = df["Estimated_arrival"].astype(str).str.replace(r'\D', '', regex=True)
    df["Estimated_arrival"] = pd.to_numeric(df["Estimated_arrival"], errors="coerce")
    
    invalid_arrival_rows = df["Estimated_arrival"].isna()
    if invalid_arrival_rows.any():
        print(f"Warning: Dropping {invalid_arrival_rows.sum()} rows with invalid 'Estimated_arrival'.")
        df = df[~invalid_arrival_rows]

    # 2. Clean Transaction Date
    #    Coerce to datetime, drop invalid
    df["Transaction_date"] = pd.to_datetime(df["Transaction_date"], errors="coerce")
    
    invalid_date_rows = df["Transaction_date"].isna()
    if invalid_date_rows.any():
         print(f"Warning: Dropping {invalid_date_rows.sum()} rows with invalid or missing 'Transaction_date'.")
         df = df[~invalid_date_rows]

    # 3. Filter out rows with missing critical IDs (Order_id)
    missing_id_mask = df["Order_id"].isna() | (df["Order_id"] == "")
    if missing_id_mask.any():
        dropped_count = missing_id_mask.sum()
        print(f"Warning: Dropping {dropped_count} rows with missing 'Order_id'.")
        df = df[~missing_id_mask]

    conn = get_db_connection()
    cur = conn.cursor()
    
    table_name = "stg_order_data"
    
    success_count = 0
    fail_count = 0
    
    # Iterate row by row to insert cleaned data
    for index, row in df.iterrows():
        try:
            insert_query = sql.SQL("""
                INSERT INTO {} (order_id, user_id, estimated_arrival, transaction_date)
                VALUES (%s, %s, %s, %s)
            """).format(sql.Identifier(table_name))
            
            cur.execute(insert_query, (
                row['Order_id'], 
                row['User_id'], 
                row['Estimated_arrival'], 
                row['Transaction_date']
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