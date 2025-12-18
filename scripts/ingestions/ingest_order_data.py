import io
from io import StringIO

import requests
import pandas as pd
import psycopg2
import pyarrow      # for parquet
import lxml         # for read_html
import openpyxl     # for read_excel


# 🔗 RAW URLs for each file
URL_2020_H1 = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Operations%20Department/order_data_20200101-20200701.parquet"
)

URL_2020_H2 = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Operations%20Department/order_data_20200701-20211001.pickle"
)

URL_2021 = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Operations%20Department/order_data_20211001-20220101.csv"
)

URL_2022 = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Operations%20Department/order_data_20220101-20221201.xlsx"
)

URL_2023_H1 = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Operations%20Department/order_data_20221201-20230601.json"
)

URL_2023_H2 = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
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


# ---------- standardization + validation ----------

def _standardize_order_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures output columns:
      order_id, user_id, estimated_arrival (INTEGER), transaction_date (TIMESTAMP)

    Cleaning rules:
    - Drop "Unnamed:*" columns
    - Normalize headers
    - estimated_arrival: keep digits only ("15days" -> 15), coerce to numeric
    - transaction_date: parse to datetime
    - Drop rows that are missing/invalid:
        order_id, user_id, estimated_arrival, transaction_date
    """
    if df is None or df.empty:
        return pd.DataFrame(columns=["order_id", "user_id", "estimated_arrival", "transaction_date"])

    df = df.copy()

    # 1) Drop junk columns
    df = df.loc[:, ~df.columns.astype(str).str.contains("^Unnamed:", case=False, regex=True)]

    # 2) Normalize headers
    rename_map = {}
    for col in df.columns:
        lc = str(col).strip().lower()
        lc_norm = lc.replace(" ", "_")

        if lc_norm == "order_id":
            rename_map[col] = "order_id"
        elif lc_norm == "user_id":
            rename_map[col] = "user_id"
        elif lc_norm == "estimated_arrival":
            rename_map[col] = "estimated_arrival"
        elif lc_norm == "transaction_date":
            rename_map[col] = "transaction_date"

    df = df.rename(columns=rename_map)

    required = ["order_id", "user_id", "estimated_arrival", "transaction_date"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns {missing}. Got {list(df.columns)}")

    df = df[required]

    # 3) Clean IDs
    df["order_id"] = df["order_id"].astype(str).str.strip()
    df["user_id"] = df["user_id"].astype(str).str.strip()

    df.loc[df["order_id"].str.lower().isin(["", "nan", "none"]), "order_id"] = None
    df.loc[df["user_id"].str.lower().isin(["", "nan", "none"]), "user_id"] = None

    # 4) Clean estimated_arrival -> integer
    arrival_digits = df["estimated_arrival"].astype(str).str.replace(r"\D", "", regex=True)
    df["estimated_arrival"] = pd.to_numeric(arrival_digits, errors="coerce")

    # Disallow +/-Infinity explicitly
    inf_arrival_mask = df["estimated_arrival"].isin([float("inf"), float("-inf")])
    if inf_arrival_mask.any():
        print(
            f"Warning: Dropping {int(inf_arrival_mask.sum())} rows with infinite estimated_arrival (inf/-inf not allowed)."
        )
        df.loc[inf_arrival_mask, "estimated_arrival"] = pd.NA

    # Prevent Postgres INTEGER overflow (max 2,147,483,647)
    too_large_arrival_mask = df["estimated_arrival"] > 2147483647
    if too_large_arrival_mask.any():
        print(
            f"Warning: Dropping {int(too_large_arrival_mask.sum())} rows with out-of-range estimated_arrival (> 2147483647)."
        )
        df.loc[too_large_arrival_mask, "estimated_arrival"] = pd.NA

    # 5) Parse transaction_date
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    # Drop invalid rows (no nulls allowed per requirement)
    invalid_mask = (
        df["order_id"].isna()
        | df["user_id"].isna()
        | df["estimated_arrival"].isna()
        | df["transaction_date"].isna()
    )
    if invalid_mask.any():
        print(f"Warning: Dropping {int(invalid_mask.sum())} invalid rows during standardization.")
        df = df.loc[~invalid_mask].copy()

    # Ensure integer type for Postgres INTEGER
    df["estimated_arrival"] = df["estimated_arrival"].astype("int64")

    # Deduplicate inside slice (extra safety)
    df = df.drop_duplicates()

    return df


# ---------- postgres helpers ----------

def _connect():
    return psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )


def _copy_df(cur, table_name: str, df: pd.DataFrame):
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
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


# ---------- main ----------

def main():
    print("⏳ Loading historical data slices...")

    df_2020_h1 = _standardize_order_df(_load_parquet(URL_2020_H1))
    df_2020_h2 = _standardize_order_df(_load_pickle(URL_2020_H2))
    df_2021    = _standardize_order_df(_load_csv(URL_2021))
    df_2022    = _standardize_order_df(_load_xlsx(URL_2022))
    df_2023_h1 = _standardize_order_df(_load_json(URL_2023_H1))
    df_2023_h2 = _standardize_order_df(_load_html(URL_2023_H2))

    print("🔗 Combining and deduplicating...")
    df_all = pd.concat(
        [df_2020_h1, df_2020_h2, df_2021, df_2022, df_2023_h1, df_2023_h2],
        ignore_index=True,
        sort=False,
    ).drop_duplicates()

    table_name = "stg_order_data"

    print("🗄️ Loading into Postgres (full refresh)...")
    conn = _connect()
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    cur.execute(
        f"""
        CREATE TABLE {table_name} (
            order_id           TEXT UNIQUE,
            user_id            TEXT,
            estimated_arrival  INTEGER,
            transaction_date   TIMESTAMP
        );
        """
    )

    if not df_all.empty:
        _copy_df(cur, table_name, df_all)

    conn.commit()
    cur.close()
    conn.close()

    print(f"✅ Loaded {len(df_all)} rows into {table_name}.")

    return {
        "table": table_name,
        "rows_loaded": int(len(df_all)),
        "sources": [
            URL_2020_H1, URL_2020_H2, URL_2021,
            URL_2022, URL_2023_H1, URL_2023_H2,
        ],
    }


if __name__ == "__main__":
    main()