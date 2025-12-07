import io
import re
import requests
import pandas as pd
import psycopg2
from io import StringIO

# 🔑 Raw URL for transactional_campaign_data.csv
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Marketing%20Department/transactional_campaign_data.csv"
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


def main():
    # 1) Download CSV from GitHub
    resp = requests.get(FILE_URL, timeout=60)
    resp.raise_for_status()

    # 2) Load into pandas
    df = pd.read_csv(io.StringIO(resp.text))

    # Drop junk index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # 3) Sanitize column names and prepare for TEXT staging table
    original_cols = list(df.columns)
    safe_cols = [_sanitize_column(c) for c in original_cols]
    df.columns = safe_cols

    # 4) Connect to Postgres (db service in docker-compose)
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    # 5) Drop & recreate staging table with all TEXT columns
    cur.execute("DROP TABLE IF EXISTS stg_transactional_campaign_data;")

    cols_sql = ",\n".join(f"{c} TEXT" for c in safe_cols)
    create_sql = f"""
        CREATE TABLE stg_transactional_campaign_data (
            {cols_sql}
        );
    """
    cur.execute(create_sql)

    # 6) Bulk insert using COPY
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    copy_sql = f"""
        COPY stg_transactional_campaign_data ({", ".join(safe_cols)})
        FROM STDIN WITH (FORMAT csv)
    """
    cur.copy_expert(copy_sql, buffer)

    conn.commit()
    cur.close()
    conn.close()

    return {
        "rows_loaded": len(df),
        "columns": safe_cols,
        "source_url": FILE_URL,
    }