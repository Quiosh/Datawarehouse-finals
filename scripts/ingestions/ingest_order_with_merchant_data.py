import io
import requests
import pandas as pd
import psycopg2
import pyarrow  # Required for read_parquet
from io import StringIO
from io import BytesIO

# 🔑 TODO: Replace these with the EXACT Raw URLs from GitHub ("Raw" button on each file)
URL_ORDER_MERCHANT_1 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Enterprise%20Department/order_with_merchant_data1.parquet"
)

URL_ORDER_MERCHANT_2 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Enterprise%20Department/order_with_merchant_data2.parquet"
)

URL_ORDER_MERCHANT_3 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Enterprise%20Department/order_with_merchant_data3.csv"
)


def _load_parquet_from_github(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    # Use BytesIO for binary parquet data
    return pd.read_parquet(BytesIO(resp.content))


def _load_csv_from_github(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes column names and removes junk columns.
    """
    # 1. Standardize headers: lowercase and strip spaces
    df.columns = df.columns.str.lower().str.strip()

    # 2. Select ONLY the required columns
    #    This automatically drops 'Unnamed: 0', 'Unnamed__0', etc.
    expected_cols = ['order_id', 'merchant_id', 'staff_id']
    
    # Validation
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing expected columns: {missing}")
        
    # Return only clean columns
    return df[expected_cols]


def main():
    # 1) Load all three datasets
    #    Note: 1 & 2 are Parquet, 3 is CSV
    try:
        # print("Loading DF1 (Parquet)...")
        df1 = _load_parquet_from_github(URL_ORDER_MERCHANT_1)
        
        # print("Loading DF2 (Parquet)...")
        df2 = _load_parquet_from_github(URL_ORDER_MERCHANT_2)
        
        # print("Loading DF3 (CSV)...")
        df3 = _load_csv_from_github(URL_ORDER_MERCHANT_3)
    except Exception as e:
        raise RuntimeError(f"Failed to download files: {e}")

    # 2) Clean and Consolidate
    #    Apply cleaning to each part
    df1 = clean_dataframe(df1)
    df2 = clean_dataframe(df2)
    df3 = clean_dataframe(df3)

    #    Merge into one big DataFrame
    df_final = pd.concat([df1, df2, df3], ignore_index=True)
    
    #    Safety: Remove any exact duplicates across the files
    df_final = df_final.drop_duplicates()

    # 3) Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    # 4) Create Single Staging Table
    #    We consolidate everything into 'stg_order_with_merchant_data'
    cur.execute("DROP TABLE IF EXISTS stg_order_with_merchant_data;")
    
    cur.execute("""
        CREATE TABLE stg_order_with_merchant_data (
            order_id     TEXT,
            merchant_id  TEXT,
            staff_id     TEXT
        );
    """)

    # 5) Bulk insert using COPY
    buffer = StringIO()
    df_final.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_order_with_merchant_data (order_id, merchant_id, staff_id)
        FROM STDIN WITH (FORMAT csv)
        """,
        buffer,
    )

    conn.commit()
    cur.close()
    conn.close()

    return {
        "status": "success",
        "table": "stg_order_with_merchant_data",
        "rows_loaded": len(df_final),
        "source_urls": [URL_ORDER_MERCHANT_1, URL_ORDER_MERCHANT_2, URL_ORDER_MERCHANT_3]
    }