import io
from urllib.parse import quote

import requests
import pandas as pd
import psycopg2
from io import StringIO

# ✅ CORRECT raw base
GITHUB_DATA_BASE = "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets"


def main():
    # 1) Build raw URL for user_job.csv
    relative_path = "Customer Management Department/user_job.csv"
    url = f"{GITHUB_DATA_BASE}/{quote(relative_path)}"

    # 2) Download CSV from GitHub
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()

    # 3) Read CSV into pandas
    df = pd.read_csv(io.StringIO(resp.text))

    # ==========================================
    # 🧹 DATA CLEANING STEPS
    # ==========================================
    
    # 1. Standardize headers: convert to lowercase and remove spaces
    #    (Fixes mismatch between "User_id" in CSV and "user_id" in DB)
    df.columns = df.columns.str.lower().str.strip()

    # 2. Remove all "Unnamed" columns (Fixes "Unnamed: 4" and "Unnamed: 0")
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]

    # 3. Trim whitespace from all string data
    #    (Ensures " Manager " becomes "Manager")
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # 4. Handle Missing Values
    #    Students have empty job_level. We fill them with "N/A" (or keep as None if preferred)
    df['job_level'] = df['job_level'].fillna('N/A')

    # ==========================================

    # Expected columns verification
    required_cols = ["user_id", "name", "job_title", "job_level"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in CSV: {missing}")

    # 4) Connect to Postgres
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
    # Write only the required columns to the buffer to ensure order
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
        "columns_found": list(df.columns)
    }