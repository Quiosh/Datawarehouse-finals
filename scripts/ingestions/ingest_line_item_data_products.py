import io
import re
import requests
import pandas as pd
import psycopg2
import pyarrow  # Required for read_parquet
from io import StringIO
from io import BytesIO

# 🔗 Raw URLs for the three Operations Department *products* files
URL_PROD_1 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/line_item_data_products1.csv"
)

URL_PROD_2 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/line_item_data_products2.csv"
)

URL_PROD_3 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/line_item_data_products3.parquet"
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
    Standardizes column names and removes junk columns.
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
        elif lc == "product_name":
            rename_map[col] = "product_name"
        elif lc == "product_id":
            rename_map[col] = "product_id"
    
    df = df.rename(columns=rename_map)

    # 3. Verify Columns
    required = ["order_id", "product_name", "product_id"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing expected columns: {missing}")

    # 4. Filter out rows with missing critical IDs
    missing_id_mask = df["order_id"].isna() | (df["order_id"] == "") | df["product_id"].isna() | (df["product_id"] == "")
    if missing_id_mask.any():
        dropped_count = missing_id_mask.sum()
        print(f"Warning: Dropping {dropped_count} rows with missing 'order_id' or 'product_id'.")
        df = df[~missing_id_mask]

    return df[required]


def main():
    # 1) Load all three datasets
    try:
        # print("Loading DF1 (CSV)...")
        df1 = _load_csv_from_github(URL_PROD_1)
        
        # print("Loading DF2 (CSV)...")
        df2 = _load_csv_from_github(URL_PROD_2)
        
        # print("Loading DF3 (Parquet)...")
        df3 = _load_parquet_from_github(URL_PROD_3)
    except Exception as e:
        raise RuntimeError(f"Failed to download files: {e}")

    # 2) Clean each dataframe
    df1 = clean_dataframe(df1)
    df2 = clean_dataframe(df2)
    df3 = clean_dataframe(df3)

    # 3) Merge into ONE big DataFrame
    #    Note: We do NOT drop duplicates here because multiple rows 
    #    with same order_id+product_id likely imply Quantity > 1.
    df_all = pd.concat([df1, df2, df3], ignore_index=True, sort=False)

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
    table_name = "stg_line_item_data_products"
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    cur.execute(f"""
        CREATE TABLE {table_name} (
            order_id      TEXT,
            product_name  TEXT,
            product_id    TEXT
        );
    """)

    # 6) Bulk insert using COPY
    buffer = StringIO()
    df_all.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY {table_name} (order_id, product_name, product_id)
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
        "sources": [URL_PROD_1, URL_PROD_2, URL_PROD_3],
    }