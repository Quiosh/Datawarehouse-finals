import io
import re
from io import StringIO

import requests
import pandas as pd
import psycopg2
import pyarrow  # needed so pandas can read parquet via pyarrow


# 🔑 Raw URLs from GitHub
URL_ORDER_MERCHANT_1 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Enterprise%20Department/order_with_merchant_data1.parquet"
)

URL_ORDER_MERCHANT_2 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Enterprise%20Department/order_with_merchant_data2.parquet"
)

URL_ORDER_MERCHANT_3 = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Enterprise%20Department/order_with_merchant_data3.csv"
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


def _load_parquet_from_github(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return pd.read_parquet(io.BytesIO(resp.content))


def _load_csv_from_github(url: str) -> pd.DataFrame:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text))


def main():
    # 1) Load all three datasets from GitHub
    df1 = _load_parquet_from_github(URL_ORDER_MERCHANT_1)
    df2 = _load_parquet_from_github(URL_ORDER_MERCHANT_2)
    df3 = _load_csv_from_github(URL_ORDER_MERCHANT_3)

    # 2) Combine into ONE big DataFrame (union of columns)
    df_all = pd.concat([df1, df2, df3], ignore_index=True, sort=False)

    # 3) Drop any junk "Unnamed: 0" style columns
    junk_cols = [c for c in df_all.columns if str(c).lower().startswith("unnamed")]
    if junk_cols:
        df_all = df_all.drop(columns=junk_cols)

    # 4) Sanitize column names once for the combined DataFrame
    original_cols = list(df_all.columns)
    safe_cols = [_sanitize_column(c) for c in original_cols]
    df_all.columns = safe_cols

    # 5) Connect once to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    table_name = "stg_order_with_merchant_data"

    # 6) Drop & recreate ONE staging table with all TEXT columns
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    cols_sql = ",\n".join(f"{col} TEXT" for col in safe_cols)
    create_sql = f"""
        CREATE TABLE {table_name} (
            {cols_sql}
        );
    """
    cur.execute(create_sql)

    # 7) Bulk insert everything using COPY
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
        "sources": [URL_ORDER_MERCHANT_1, URL_ORDER_MERCHANT_2, URL_ORDER_MERCHANT_3],
        "dropped_unnamed_columns": junk_cols,
    }