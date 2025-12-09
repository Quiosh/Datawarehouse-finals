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
    
    # 1. Standardize headers
    df.columns = df.columns.str.lower().str.strip()

    # 2. Remove "Unnamed" columns
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]

    # 3. Trim whitespace
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # 4. Handle Missing Values
    df['job_level'] = df['job_level'].fillna('N/A')

    # ------------------------------------------
    # 🚀 SOFT DEDUPLICATION LOGIC
    # ------------------------------------------
    # Since there is no date, we assume the LAST row in the file is the "Latest".
    
    # A. Create a temp index to track file order
    df = df.reset_index()

    # B. Sort by User ID and Index (Descending) -> Newest file entry comes first
    df = df.sort_values(by=['user_id', 'index'], ascending=[True, False])

    # C. Flag Duplicates
    #    keep='first' means the top row (the latest one) is NOT a duplicate.
    #    All others are marked True.
    df['possible_duplicate'] = df.duplicated(subset=['user_id'], keep='first')

    # D. Link to Master ID
    df['possible_duplicate_of'] = None
    # If it is a duplicate, point it to the user_id (the master's ID)
    df.loc[df['possible_duplicate'], 'possible_duplicate_of'] = df['user_id']

    # E. Clean up temp columns
    df = df.drop(columns=['index'])
    # ==========================================

    # Expected columns verification
    required_cols = ["user_id", "name", "job_title", "job_level", "possible_duplicate", "possible_duplicate_of"]
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

    # 5) Drop & Recreate staging table (FIXED)
    cur.execute("DROP TABLE IF EXISTS stg_user_job;")
    
    cur.execute("""
        CREATE TABLE stg_user_job (
            user_id               TEXT,
            name                  TEXT,
            job_title             TEXT,
            job_level             TEXT,
            possible_duplicate    BOOLEAN,
            possible_duplicate_of TEXT
        );
    """)

    # 6) Bulk insert using COPY
    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_user_job (
            user_id, name, job_title, job_level, 
            possible_duplicate, possible_duplicate_of
        )
        FROM STDIN WITH (FORMAT csv)
        """,
        buffer,
    )

    conn.commit()
    cur.close()
    conn.close()

    return {
        "rows_loaded": len(df),
        "duplicates_flagged": int(df['possible_duplicate'].sum()),
        "source_url": url,
    }