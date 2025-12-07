import io
from urllib.parse import quote

import requests
import pandas as pd
import psycopg2
from io import StringIO

# ✅ CORRECT raw base (no /tree/, no github.com, no $0)
GITHUB_DATA_BASE = "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets"


def main():
    # 1) Build raw URL for user_job.csv in Customer Management Department
    relative_path = "Customer Management Department/user_job.csv"
    # quote() will encode spaces -> %20 but keep slashes
    url = f"{GITHUB_DATA_BASE}/{quote(relative_path)}"

    # Debug: you can print or return this if needed
    # print("Fetching:", url)

    # 2) Download CSV from GitHub
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()  # <- will only raise if truly 404/500

    # 3) Read CSV into pandas
    df = pd.read_csv(io.StringIO(resp.text))

    # Drop junk column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Expected columns
    required_cols = ["user_id", "name", "job_title", "job_level"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in CSV: {missing}")

    # 4) Connect to Postgres (your db service)
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    # 5) Create / reset staging table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_user_job (
            user_id   TEXT,
            name      TEXT,
            job_title TEXT,
            job_level TEXT
        );
        TRUNCATE TABLE stg_user_job;
    """)

    # 6) Bulk insert using COPY
    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_user_job (user_id, name, job_title, job_level)
        FROM STDIN WITH (FORMAT csv)
        """,
        buffer,
    )

    conn.commit()
    cur.close()
    conn.close()

    return {
        "rows_loaded": len(df),
        "source_url": url,
    }