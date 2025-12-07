import io
import requests
import pandas as pd
import psycopg2
from io import StringIO
import lxml

# 🔗 Raw URL for order_delays.html
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/order_delays.html"
)

def main():
    # 1) Download HTML from GitHub
    resp = requests.get(FILE_URL, timeout=60)
    resp.raise_for_status()

    # 2) Parse HTML Table
    #    We use read_html because the source is an HTML file
    tables = pd.read_html(resp.text)
    if not tables:
        raise ValueError("No tables found in order_delays HTML")
    
    df = tables[0]

    # ==========================================
    # 🧹 DATA CLEANING STEPS
    # ==========================================

    # 1. Standardize Headers
    #    - Lowercase: "Order ID" -> "order id"
    #    - Replace spaces with underscores: "order id" -> "order_id"
    #      (This handles "Delay in Days" -> "delay_in_days")
    df.columns = df.columns.str.lower().str.strip().str.replace(' ', '_')

    # 2. Remove Junk Columns
    #    Removes "Unnamed: 2", "Unnamed: 0", etc.
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]

    # 3. Clean "delay_in_days"
    #    Ensure it is numeric (integers). Coerce errors to NaN.
    if "delay_in_days" in df.columns:
        df["delay_in_days"] = pd.to_numeric(df["delay_in_days"], errors="coerce")


    # Verify Columns
    required_cols = ["order_id", "delay_in_days"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing expected columns {missing}. Got {list(df.columns)}"
        )

    # 3) Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    table_name = "stg_order_delays"

    # 4) Drop & Recreate Staging Table
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    
    cur.execute(f"""
        CREATE TABLE {table_name} (
            order_id       TEXT,
            delay_in_days  INTEGER
        );
    """)

    # 5) Bulk insert using COPY
    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY {table_name} (
            order_id,
            delay_in_days
        )
        FROM STDIN WITH (FORMAT csv)
        """,
        buffer,
    )

    conn.commit()
    cur.close()
    conn.close()

    return {
        "table": table_name,
        "rows_loaded": len(df),
        "source_url": FILE_URL,
        "columns_found": list(df.columns)
    }