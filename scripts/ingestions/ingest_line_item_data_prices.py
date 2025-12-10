import io
import re
import requests
import pandas as pd
import psycopg2
import pyarrow  # Required for read_parquet
from io import StringIO
from io import BytesIO

# 🔑 Raw URLs for the three Operations Department files
URL_PRICES_1 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/line_item_data_prices1.csv"
)

URL_PRICES_2 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/line_item_data_prices2.csv"
)

URL_PRICES_3 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/line_item_data_prices3.parquet"
)


def _load_csv_from_github(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))


def _load_parquet_from_github(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    # Use BytesIO for binary parquet data
    return pd.read_parquet(io.BytesIO(resp.content))


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes column names, cleans data, and removes junk columns.
    """
    # 1. Drop junk columns (Unnamed: 0, Unnamed__0, etc)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False)]

    # 2. Normalize Headers
    #    "Order_id" -> "order_id"
    rename_map = {}
    for col in df.columns:
        lc = str(col).strip().lower()
        if lc == "order_id":
            rename_map[col] = "order_id"
        elif lc == "price":
            rename_map[col] = "price"
        elif lc == "quantity":
            rename_map[col] = "quantity"
    
    df = df.rename(columns=rename_map)

    # 3. Verify Columns
    required = ["order_id", "price", "quantity"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing expected columns: {missing}")

    # 4. Clean Quantity
    #    "6pieces", "6px", "4PC" -> 6 (Integer)
    #    Regex strips everything that is NOT a digit
    df["quantity"] = df["quantity"].astype(str).str.replace(r'\D', '', regex=True)
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    # 5. Clean Price
    #    Ensure numeric (Float/Numeric)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")

    return df[required]


def main():
    # 1) Load all three datasets
    try:
        # print("Loading DF1 (CSV)...")
        df1 = _load_csv_from_github(URL_PRICES_1)
        
        # print("Loading DF2 (CSV)...")
        df2 = _load_csv_from_github(URL_PRICES_2)
        
        # print("Loading DF3 (Parquet)...")
        df3 = _load_parquet_from_github(URL_PRICES_3)
    except Exception as e:
        raise RuntimeError(f"Failed to download files: {e}")

    # 2) Clean each dataframe
    df1 = clean_dataframe(df1)
    df2 = clean_dataframe(df2)
    df3 = clean_dataframe(df3)

    # 3) Merge into ONE big DataFrame
    #    Union of all rows
    df_all = pd.concat([df1, df2, df3], ignore_index=True, sort=False)
    
    #    (Optional) Deduplication:
    #    We avoid drop_duplicates() here just in case multiple line items 
    #    have identical price/quantity for the same order (rare but possible).
    #    If you want strictly unique rows, uncomment the next line:
    #    df_all = df_all.drop_duplicates()

    # 4) Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    # 5) Drop & recreate staging table
    table_name = "stg_line_item_data_prices"
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    #    Note: price is NUMERIC, quantity is INTEGER
    cur.execute(f"""
        CREATE TABLE {table_name} (
            order_id  TEXT,
            price     NUMERIC,
            quantity  INTEGER
        );
    """)

    # 6) Bulk insert using COPY
    buffer = StringIO()
    df_all.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY {table_name} (order_id, price, quantity)
        FROM STDIN WITH (FORMAT csv)
        """,
        buffer,
    )

    conn.commit()
    cur.close()
    conn.close()

    return {
        "table": table_name,
        "rows_loaded": len(df_all),
        "sources": [URL_PRICES_1, URL_PRICES_2, URL_PRICES_3],
    }