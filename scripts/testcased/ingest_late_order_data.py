import io
from io import StringIO
import re  # Added for regex cleaning

import requests
import pandas as pd
import psycopg2
import pyarrow  # for parquet
import lxml  # for read_html
import openpyxl  # for read_excel

# SELECT COUNT(*) FROM fact_orders WHERE date_key = 20240102; to read after state

# Local test file (used as default if no upload is provided)
URL_TEST_FILE = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Test%20Files/late_orders.csv"
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


def _load_local_csv(path: str) -> pd.DataFrame:
    """Load a local CSV file."""
    return pd.read_csv(path)


# ---------- standardization ----------


def _standardize_order_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Make sure we end up with columns:
      order_id, user_id, estimated_arrival (INTEGER), transaction_date (TIMESTAMP)
    """
    # 1. Drop junk columns like "Unnamed: 0"
    df = df.loc[:, ~df.columns.str.contains("^Unnamed:", case=False)]

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
    df["estimated_arrival"] = (
        df["estimated_arrival"].astype(str).str.replace(r"\D", "", regex=True)
    )
    df["estimated_arrival"] = pd.to_numeric(df["estimated_arrival"], errors="coerce")

    # 4. Parse transaction_date as timestamp
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    return df


# ---------- main ----------


def main(new_orders_file: bytes = None):
    """
    Ingest ONLY the new test data and append it to the existing staging table.

    Args:
        new_orders_file: File upload from Windmill (passed as bytes).
    """

    df_new_orders = pd.DataFrame()

    # 1) Load new orders file (Upload or Fallback)
    if new_orders_file:
        print("Processing uploaded file...")
        try:
            # Windmill passes file uploads as bytes.
            # We must wrap it in BytesIO so pandas can read it like a file.
            file_stream = io.BytesIO(new_orders_file)
            df_new_orders = _standardize_order_df(pd.read_csv(file_stream))
            print(f"Successfully loaded {len(df_new_orders)} rows from uploaded file.")
        except Exception as e:
            print(f"Error reading uploaded file: {e}")
            raise e
    else:
        # Default to loading the test file from the defined local path/URL
        print("No upload provided. Attempting to load default test file...")
        try:
            # We use _load_local_csv here since it is a local file path
            df_new_orders = _standardize_order_df(_load_local_csv(URL_TEST_FILE))
            print(
                f"Successfully loaded {len(df_new_orders)} rows from default test file."
            )
        except Exception as e:
            print(f"Could not load default test file: {e}")
            # If no data found, we can just return or raise, but let's proceed with empty df
            pass

    if df_new_orders.empty:
        print("No new data to append.")
        return {"rows_loaded": 0}

    # 2) Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    table_name = "stg_order_data"

    # 3) Bulk insert (APPEND) using COPY
    # We do NOT drop/create the table. We assume it exists (created by main ingestion script).
    buffer = StringIO()
    df_new_orders.to_csv(buffer, index=False, header=False)
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
        "rows_loaded": len(df_new_orders),
        "note": "Appended to existing table",
    }


if __name__ == "__main__":
    main()
