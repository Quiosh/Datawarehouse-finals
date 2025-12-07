import io
from io import StringIO
import re  # Added for regex cleaning

import requests
import pandas as pd
import psycopg2
import pyarrow      # for parquet
import lxml         # for read_html
import openpyxl     # for read_excel


# 🔗 RAW URLs for each file
URL_2020_H1 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/order_data_20200101-20200701.parquet"
)

URL_2020_H2 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/order_data_20200701-20211001.pickle"
)

URL_2021 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/order_data_20211001-20220101.csv"
)

URL_2022 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/order_data_20220101-20221201.xlsx"
)

URL_2023_H1 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/order_data_20221201-20230601.json"
)

URL_2023_H2 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Operations%20Department/order_data_20230601-20240101.html"
)


# ---------- helpers to download + load ----------

def _get(url: str) -> requests.Response:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp


def _load_parquet(url: str) -> pd.DataFrame:
    resp = _get(url)
    return pd.read_parquet(io.BytesIO(resp.content))


def _load_pickle(url: str) -> pd.DataFrame:
    resp = _get(url)
    return pd.read_pickle(io.BytesIO(resp.content))


def _load_csv(url: str) -> pd.DataFrame:
    resp = _get(url)
    return pd.read_csv(StringIO(resp.text))


def _load_xlsx(url: str) -> pd.DataFrame:
    resp = _get(url)
    return pd.read_excel(io.BytesIO(resp.content), engine="openpyxl")


def _load_json(url: str) -> pd.DataFrame:
    resp = _get(url)
    return pd.read_json(io.BytesIO(resp.content))


def _load_html(url: str) -> pd.DataFrame:
    resp = _get(url)
    tables = pd.read_html(resp.text)
    if not tables:
        raise ValueError(f"No tables found in HTML from {url}")
    return tables[0]


# ---------- standardization ----------

def _standardize_order_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure we end up with columns:
      order_id, user_id, estimated_arrival (INTEGER), transaction_date (TIMESTAMP)
    """
    # 1. Drop junk columns like "Unnamed: 0"
    df = df.loc[:, ~df.columns.str.contains('^Unnamed:', case=False)]

    # 2. Normalize headers
    rename_map = {}
    for col in df.columns:
        lc = str(col).strip().lower()
        if lc.replace(" ", "_") == "estimated_arrival":
            rename_map[col] = "estimated_arrival"
        elif lc == "order_id":
            rename_map[col] = "order_id"
        elif lc == "user_id":
            rename_map[col] = "user_id"
        elif lc == "transaction_date":
            rename_map[col] = "transaction_date"

    df = df.rename(columns=rename_map)

    required = ["order_id", "user_id", "estimated_arrival", "transaction_date"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns {missing}. Got {list(df.columns)}")

    # Keep only required columns, in order
    df = df[required]

    # 3. Clean estimated_arrival
    #    "15days" -> 15 (Integer)
    #    Regex strips everything that is NOT a digit
    df["estimated_arrival"] = df["estimated_arrival"].astype(str).str.replace(r'\D', '', regex=True)
    df["estimated_arrival"] = pd.to_numeric(df["estimated_arrival"], errors="coerce")

    # 4. Parse transaction_date as timestamp
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    return df


# ---------- main ----------

def main():
    # 1) Load each slice
    df_2020_h1 = _standardize_order_df(_load_parquet(URL_2020_H1))
    df_2020_h2 = _standardize_order_df(_load_pickle(URL_2020_H2))
    df_2021    = _standardize_order_df(_load_csv(URL_2021))
    df_2022    = _standardize_order_df(_load_xlsx(URL_2022))
    df_2023_h1 = _standardize_order_df(_load_json(URL_2023_H1))
    df_2023_h2 = _standardize_order_df(_load_html(URL_2023_H2))

    # 2) Combine into one big DataFrame
    df_all = pd.concat(
        [df_2020_h1, df_2020_h2, df_2021, df_2022, df_2023_h1, df_2023_h2],
        ignore_index=True,
        sort=False,
    )

    # 3) Safety Deduplication
    #    Remove rows that might overlap between files
    df_all = df_all.drop_duplicates()

    # 4) Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    table_name = "stg_order_data"

    # 5) Drop & recreate staging table
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    
    # Changed estimated_arrival to INTEGER
    cur.execute(f"""
        CREATE TABLE {table_name} (
            order_id           TEXT,
            user_id            TEXT,
            estimated_arrival  INTEGER,
            transaction_date   TIMESTAMP
        );
    """)

    # 6) Bulk insert using COPY
    buffer = StringIO()
    df_all.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY {table_name} (
            order_id,
            user_id,
            estimated_arrival,
            transaction_date
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
        "rows_loaded": len(df_all),
        "sources": [
            URL_2020_H1, URL_2020_H2, URL_2021,
            URL_2022, URL_2023_H1, URL_2023_H2,
        ],
    }