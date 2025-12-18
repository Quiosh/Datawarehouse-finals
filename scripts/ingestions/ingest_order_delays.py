import requests
import pandas as pd
import psycopg2
from io import StringIO
import lxml  # ensure lxml is available for read_html


# 🔗 Raw URL for order_delays.html
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Operations%20Department/order_delays.html"
)


def main():
    # 1) Download HTML from GitHub
    resp = requests.get(FILE_URL, timeout=60)
    resp.raise_for_status()
    html_str = resp.text

    # 2) Parse first table from HTML
    tables = pd.read_html(html_str)
    if not tables:
        raise ValueError("No tables found in order_delays HTML")

    df = tables[0]

    # Drop junk index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # 3) Normalize column names
    rename_map = {}
    for col in df.columns:
        lc = str(col).strip().lower()
        if lc == "order_id":
            rename_map[col] = "order_id"
        elif lc.replace(" ", "_") == "delay_in_days":
            rename_map[col] = "delay_in_days"

    df = df.rename(columns=rename_map)

    required_cols = ["order_id", "delay_in_days"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing expected columns {missing}. Got {list(df.columns)}"
        )

    # 4) Cast delay_in_days to integer-like
    df["delay_in_days"] = pd.to_numeric(df["delay_in_days"], errors="coerce").astype("Int64")

    # 5) Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    table_name = "stg_order_delays"

    # 6) Drop & recreate staging table
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    cur.execute(f"""
        CREATE TABLE {table_name} (
            order_id       TEXT,
            delay_in_days  INTEGER
        );
    """)

    # 7) Bulk insert using COPY
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
    }