import io
import re
import requests
import pandas as pd
import psycopg2
from io import StringIO

# 🔑 Raw URL
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Marketing%20Department/transactional_campaign_data.csv"
)


def main():
    # 1) Download CSV from GitHub
    resp = requests.get(FILE_URL, timeout=60)
    resp.raise_for_status()

    # 2) Load into pandas
    df = pd.read_csv(io.StringIO(resp.text))

    # ==========================================
    # 🧹 DATA CLEANING STEPS
    # ==========================================

    # 1. Drop Junk Columns (Unnamed: 0, Unnamed: 5, etc.)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed', case=False)]

    # 2. Normalize Headers
    #    "Campaign_id" -> "campaign_id"
    rename_map = {}
    for col in df.columns:
        lc = str(col).strip().lower()
        if lc == "campaign_id":
            rename_map[col] = "campaign_id"
        elif lc == "order_id":
            rename_map[col] = "order_id"
        elif lc == "transaction_date":
            rename_map[col] = "transaction_date"
        elif lc.replace(" ", "_") == "estimated_arrival":
            rename_map[col] = "estimated_arrival"
        elif lc == "availed":
            rename_map[col] = "availed"

    df = df.rename(columns=rename_map)

    # 3. Clean Estimated Arrival
    #    "5days" -> 5 (Integer)
    if "estimated_arrival" in df.columns:
        df["estimated_arrival"] = df["estimated_arrival"].astype(str).str.replace(r'\D', '', regex=True)
        df["estimated_arrival"] = pd.to_numeric(df["estimated_arrival"], errors="coerce")

    # 4. Clean Transaction Date (Remove 00:00:00)
    #    We convert to datetime, then format as string YYYY-MM-DD
    if "transaction_date" in df.columns:
        df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce").dt.date

    # 5. Clean Availed
    if "availed" in df.columns:
        df["availed"] = pd.to_numeric(df["availed"], errors="coerce").fillna(0).astype(int)

    # 6. Safety Deduplication
    df = df.drop_duplicates()

    # ==========================================

    # Verify Columns
    required_cols = ["transaction_date", "campaign_id", "order_id", "estimated_arrival", "availed"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns {missing}. Got {list(df.columns)}")

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
    table_name = "stg_transactional_campaign_data"
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    #    Note: transaction_date is now DATE (no time)
    cur.execute(f"""
        CREATE TABLE {table_name} (
            transaction_date   DATE,
            campaign_id        TEXT,
            order_id           TEXT,
            estimated_arrival  INTEGER,
            availed            INTEGER
        );
    """)

    # 6) Bulk insert using COPY
    buffer = StringIO()
    cols_to_write = ["transaction_date", "campaign_id", "order_id", "estimated_arrival", "availed"]
    df[cols_to_write].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY {table_name} (
            transaction_date, 
            campaign_id, 
            order_id, 
            estimated_arrival, 
            availed
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
        "columns_final": cols_to_write
    }