import io
import requests
import pandas as pd
import psycopg2
from io import StringIO
import lxml 
import re # Added regex library

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

    # 4. Safety Deduplication
    df = df.drop_duplicates()

    # 5. Parse Dates
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

    # 6. Clean Phone Numbers (NEW STEP)
    #    Removes '(', ')', '.', and '-' to leave only digits
    if "contact_number" in df.columns:
        df["contact_number"] = df["contact_number"].astype(str).str.replace(r'[().-]', '', regex=True)
        # Optional: If you want to strip the leading '1' from '15551234' to match others, 
        # you can decide that here. For now, we just standardize to digits.

    # ==========================================

    # Verify Columns
    required_cols = [
        "staff_id", "name", "job_level", "street",
        "state", "city", "country", "contact_number", "creation_date"
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_staff_data (
            staff_id        TEXT,
            name            TEXT,
            job_level       TEXT,
            street          TEXT,
            state           TEXT,
            city            TEXT,
            country         TEXT,
            contact_number  TEXT,
            creation_date   TIMESTAMP
        );
        TRUNCATE TABLE stg_staff_data;
    """)

    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_staff_data (
            staff_id, name, job_level, street, state, city, 
            country, contact_number, creation_date
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
        "source_url": FILE_URL
    }