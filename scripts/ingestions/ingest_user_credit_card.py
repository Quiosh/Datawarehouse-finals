import requests
import pandas as pd
import psycopg2
from io import BytesIO, StringIO

# 🔑 Replace with the EXACT Raw URL for user_credit_card.pickle from GitHub
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Customer%20Management%20Department/user_credit_card.pickle"
)


def main():
    # 1) Download pickle from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()

    # 2) Load pickled DataFrame from bytes
    #    If the file on GitHub is a pickle, this works.
    try:
        df = pd.read_pickle(BytesIO(resp.content))
    except Exception:
        # Fallback: If it's actually a CSV disguised as a pickle (rare but possible)
        try:
            df = pd.read_csv(BytesIO(resp.content))
        except Exception as e:
            raise ValueError(f"Failed to load file. Error: {e}")

    # ==========================================
    # 🧹 DATA CLEANING STEPS
    # ==========================================

    # 1. Standardize Headers
    #    "User_id" -> "user_id"
    df.columns = df.columns.str.lower().str.strip()

    # 2. Drop Junk Columns (Unnamed: 4, etc.)
    df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]

    # 3. Clean Credit Card Number
    #    Ensure it is a string (safe for DB, prevents scientific notation)
    if "credit_card_number" in df.columns:
        df["credit_card_number"] = df["credit_card_number"].astype(str).str.strip()
        # Remove '.0' if it was loaded as a float
        df["credit_card_number"] = df["credit_card_number"].str.replace(r'\.0$', '', regex=True)

    # 4. Safety Deduplication
    df = df.drop_duplicates()

    # ==========================================

    # Expected columns check
    required_cols = ["user_id", "name", "credit_card_number", "issuing_bank"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing expected columns: {missing}. "
            f"Available columns: {list(df.columns)}"
        )

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
        CREATE TABLE IF NOT EXISTS stg_user_credit_card (
            user_id             TEXT,
            name                TEXT,
            credit_card_number  TEXT,
            issuing_bank        TEXT
        );
        TRUNCATE TABLE stg_user_credit_card;
    """)

    # 5) Bulk insert using COPY
    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_user_credit_card (
            user_id,
            name,
            credit_card_number,
            issuing_bank
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