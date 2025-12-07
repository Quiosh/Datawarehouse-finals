import io
import re
import requests
import pandas as pd
import psycopg2
from io import StringIO
import lxml  # ensure lxml is available for read_html

# 🔑 Replace with the EXACT Raw URL for merchant_data.html from GitHub
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Enterprise%20Department/merchant_data.html"
)


def main():
    # 1) Download HTML from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()

    # 2) Parse HTML Table
    #    We use read_html as the primary parser
    tables = pd.read_html(resp.text)
    if not tables:
        raise ValueError("No tables found in merchant_data HTML")
    
    df = tables[0]

    # ==========================================
    # 🧹 DATA CLEANING STEPS
    # ==========================================

    # 1. Standardize Headers
    #    "Merchant_id" -> "merchant_id"
    df.columns = df.columns.str.lower().str.strip()

    # 2. Drop Junk Columns (Unnamed: 0, Unnamed: 8, etc.)
    df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]

    # 3. Clean Contact Number
    #    Remove non-digits: "(452) 170-5656" -> "4521705656"
    if "contact_number" in df.columns:
        df["contact_number"] = df["contact_number"].astype(str).str.replace(r'\D', '', regex=True)

    # 4. Parse Dates
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

    # 5. Trim Whitespace from Text
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # 6. Safety Deduplication (Exact rows only)
    df = df.drop_duplicates()

    # ==========================================

    # Expected columns verification
    required_cols = [
        "merchant_id", "creation_date", "name", "street", "state", 
        "city", "country", "contact_number"
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

    # 4) Create / Reset Staging Table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS stg_merchant_data (
            merchant_id     TEXT,
            creation_date   TIMESTAMP,
            name            TEXT,
            street          TEXT,
            state           TEXT,
            city            TEXT,
            country         TEXT,
            contact_number  TEXT
        );
        TRUNCATE TABLE stg_merchant_data;
    """)

    # 5) Bulk insert using COPY
    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_merchant_data (
            merchant_id,
            creation_date,
            name,
            street,
            state,
            city,
            country,
            contact_number
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
        "source_url": FILE_URL,
        "columns_found": list(df.columns)
    }