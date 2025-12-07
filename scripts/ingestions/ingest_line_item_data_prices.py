import io
import re
from io import StringIO

import requests
import pandas as pd
import psycopg2
import pyarrow  # needed so pandas can read parquet via pyarrow


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


def _sanitize_column(name: str) -> str:
    """
    Turn any column name into a safe Postgres identifier:
    - lower case
    - spaces and weird chars -> _
    - prefix with _ if it starts with a digit
    """
    col = str(name).strip().lower()
    col = re.sub(r"[^a-z0-9_]", "_", col)
    if re.match(r"^[0-9]", col):
        col = "_" + col
    if col == "":
        col = "col"
    return col


def _load_csv_from_github(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))


def _load_parquet_from_github(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return pd.read_parquet(io.BytesIO(resp.content))


def main():
    # 1) Load all three datasets from GitHub
    df_prices1 = _load_csv_from_github(URL_PRICES_1)
    df_prices2 = _load_csv_from_github(URL_PRICES_2)
    df_prices3 = _load_parquet_from_github(URL_PRICES_3)

    # 2) Merge them into ONE big DataFrame
    df_all = pd.concat([df_prices1, df_prices2, df_prices3],
                       ignore_index=True, sort=False)

    # 3) Sanitize column names once for the combined DataFrame
    original_cols = list(df_all.columns)
    safe_cols = [_sanitize_column(c) for c in original_cols]
    df_all.columns = safe_cols

    # 4) Connect once to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    # 5) Drop & recreate ONE staging table with all TEXT columns
    table_name = "stg_line_item_data_prices"
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    cols_sql = ",\n".join(f"{col} TEXT" for col in safe_cols)
    create_sql = f"""
        CREATE TABLE {table_name} (
            {cols_sql}
        );
    """
    cur.execute(create_sql)

    # 6) Bulk insert everything using COPY
    buffer = StringIO()
    df_all.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    copy_sql = f"""
        COPY {table_name} ({", ".join(safe_cols)})
        FROM STDIN WITH (FORMAT csv)
    """
    cur.copy_expert(copy_sql, buffer)

    conn.commit()
    cur.close()
    conn.close()

    return {
        "table": table_name,
        "rows_loaded": len(df_all),
        "columns": safe_cols,
        "sources": [URL_PRICES_1, URL_PRICES_2, URL_PRICES_3],
    }