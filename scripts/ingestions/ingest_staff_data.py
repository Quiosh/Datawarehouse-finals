import io
import requests
import pandas as pd
import psycopg2
from io import StringIO
import lxml 
import re 

# 🔑 Replace with the EXACT Raw URL of staff_data.html from GitHub
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Enterprise%20Department/staff_data.html"
)

def main():
    # 1. Download & Parse
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()
    
    tables = pd.read_html(resp.text)
    if not tables:
        raise ValueError("No tables found")
    df = tables[0]

    # ==========================================
    # 🧹 DATA CLEANING STEPS
    # ==========================================

    # 1. Standardize headers
    df.columns = df.columns.str.lower().str.strip()

    # 2. Remove junk columns
    df = df.loc[:, ~df.columns.str.contains('^unnamed')]

    # 3. Trim whitespace
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # 4. Parse Dates
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

    # 5. Clean Phone Numbers
    if "contact_number" in df.columns:
        df["contact_number"] = df["contact_number"].astype(str).str.replace(r'\D', '', regex=True)

    # ------------------------------------------
    # 🚀 SOFT DEDUPLICATION LOGIC
    # ------------------------------------------
    
    # A. Sort by Staff ID and Creation Date (Newest first)
    df = df.sort_values(by=['staff_id', 'creation_date'], ascending=[True, False])

    # B. Flag Duplicates
    #    keep='first' preserves the newest record as False (Not duplicate)
    df['possible_duplicate'] = df.duplicated(subset=['staff_id'], keep='first')

    # C. Link to Master ID
    df['possible_duplicate_of'] = None
    df.loc[df['possible_duplicate'], 'possible_duplicate_of'] = df['staff_id']

    # ==========================================

    # Verify Columns
    required_cols = [
        "staff_id", "name", "job_level", "street", "state", "city", 
        "country", "contact_number", "creation_date", 
        "possible_duplicate", "possible_duplicate_of"
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    # Connect & Load
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    # DROP TABLE fix
    cur.execute("DROP TABLE IF EXISTS stg_staff_data;")

    cur.execute("""
        CREATE TABLE stg_staff_data (
            staff_id              TEXT,
            name                  TEXT,
            job_level             TEXT,
            street                TEXT,
            state                 TEXT,
            city                  TEXT,
            country               TEXT,
            contact_number        TEXT,
            creation_date         TIMESTAMP,
            possible_duplicate    BOOLEAN,
            possible_duplicate_of TEXT
        );
    """)

    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_staff_data (
            staff_id, name, job_level, street, state, city, 
            country, contact_number, creation_date,
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
        "source_url": FILE_URL
    }