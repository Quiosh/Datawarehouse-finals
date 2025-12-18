import json
import requests
import pandas as pd
import psycopg2
from io import StringIO

# 🔑 Replace this with the EXACT Raw URL for user_data.json from GitHub
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/datasets/Customer%20Management%20Department/user_data.json"
)


def main():
    # 1) Download JSON from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()

    # 2) Parse JSON
    raw = json.loads(resp.text)
    cols = {col: pd.Series(mapping) for col, mapping in raw.items()}
    df = pd.DataFrame(cols)

    # ==========================================
    # 🧹 DATA CLEANING STEPS
    # ==========================================

    # 1. Standardize Headers
    df.columns = df.columns.str.lower().str.strip()

    # 2. Drop Junk Columns
    df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]

    # 3. Parse Dates
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
    if "birthdate" in df.columns:
        df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")

    # 4. Trim Whitespace
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # ------------------------------------------
    # 🚀 SOFT DEDUPLICATION LOGIC
    # ------------------------------------------
    
    # A. Sort by User ID and Creation Date (Newest first)
    df = df.sort_values(by=['user_id', 'creation_date'], ascending=[True, False])

    # B. Flag Duplicates
    #    keep='first' preserves the newest record as the Master.
    df['possible_duplicate'] = df.duplicated(subset=['user_id'], keep='first')

    # C. Link to Master ID
    df['possible_duplicate_of'] = None
    df.loc[df['possible_duplicate'], 'possible_duplicate_of'] = df['user_id']

    # ==========================================

    # Verify Columns
    required_cols = [
        "user_id", "creation_date", "name", "street", "state", 
        "city", "country", "birthdate", "gender", "device_address", "user_type",
        "possible_duplicate", "possible_duplicate_of"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in JSON: {missing}")

    # 3) Connect directly to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    # 4) Drop & Recreate Staging Table (FIXED)
    cur.execute("DROP TABLE IF EXISTS stg_user_data;")

    cur.execute("""
        CREATE TABLE stg_user_data (
            user_id               TEXT,
            creation_date         TIMESTAMP,
            name                  TEXT,
            street                TEXT,
            state                 TEXT,
            city                  TEXT,
            country               TEXT,
            birthdate             DATE,
            gender                TEXT,
            device_address        TEXT,
            user_type             TEXT,
            possible_duplicate    BOOLEAN,
            possible_duplicate_of TEXT
        );
    """)

    # 5) Bulk insert using COPY
    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_user_data (
            user_id, creation_date, name, street, state, city, country, 
            birthdate, gender, device_address, user_type,
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
        "source_url": FILE_URL,
    }