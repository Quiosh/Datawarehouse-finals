import requests
import pandas as pd
import psycopg2
from io import BytesIO, StringIO

# 🔑 Replace with the EXACT Raw URL for user_credit_card.pickle from GitHub
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/datasets/Customer%20Management%20Department/user_credit_card.pickle"
)


def main():
    # 1) Download pickle from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()  # raise if 404/500

    # 2) Load pickled DataFrame from bytes
    df = pd.read_pickle(BytesIO(resp.content))

    # Expected columns based on the pickle:
    # user_id, name, credit_card_number, issuing_bank
    required_cols = ["user_id", "name", "credit_card_number", "issuing_bank"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in pickle: {missing}")

    # Optional: ensure credit card number is a string (safer than int)
    df["credit_card_number"] = df["credit_card_number"].astype(str)

    # 3) Connect directly to Postgres container "db"
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    # 4) Create / reset staging table
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
    }