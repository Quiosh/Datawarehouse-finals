import io
import re
import requests
import pandas as pd
import psycopg2
from io import StringIO
import lxml 

# 🔑 Replace with the EXACT Raw URL for merchant_data.html from GitHub
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Enterprise%20Department/merchant_data.html"
)


def main():
    # 1) Download HTML from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()

    # 2) Parse HTML Table
    tables = pd.read_html(resp.text)
    if not tables:
        raise ValueError("No tables found in merchant_data HTML")
    
    df = tables[0]

    # ==========================================
    # 🧹 DATA CLEANING STEPS
    # ==========================================

    # 1. Standardize Headers
    df.columns = df.columns.str.lower().str.strip()

    # 2. Drop Junk Columns
    df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]

    # 3. Clean Contact Number
    if "contact_number" in df.columns:
        df["contact_number"] = df["contact_number"].astype(str).str.replace(r'\D', '', regex=True)

    # 4. Parse Dates
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

    # 5. Trim Whitespace
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # ------------------------------------------
    # 🚀 SOFT DEDUPLICATION LOGIC
    # ------------------------------------------
    
    # A. Sort by Merchant ID and Creation Date (Newest first)
    df = df.sort_values(by=['merchant_id', 'creation_date'], ascending=[True, False])

    # B. Flag Duplicates
    #    keep='first' preserves the newest record as the Master.
    df['possible_duplicate'] = df.duplicated(subset=['merchant_id'], keep='first')

    # C. Link to Master ID
    df['possible_duplicate_of'] = None
    df.loc[df['possible_duplicate'], 'possible_duplicate_of'] = df['merchant_id']

    # ==========================================

    # Expected columns verification
    required_cols = [
        "merchant_id", "creation_date", "name", "street", "state", 
        "city", "country", "contact_number", 
        "possible_duplicate", "possible_duplicate_of"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in HTML: {missing}")

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
    cur.execute("DROP TABLE IF EXISTS stg_merchant_data;")

    cur.execute("""
        CREATE TABLE stg_merchant_data (
            merchant_id           TEXT,
            creation_date         TIMESTAMP,
            name                  TEXT,
            street                TEXT,
            state                 TEXT,
            city                  TEXT,
            country               TEXT,
            contact_number        TEXT,
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
        COPY stg_merchant_data (
            merchant_id, creation_date, name, street, state, city, country, contact_number,
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