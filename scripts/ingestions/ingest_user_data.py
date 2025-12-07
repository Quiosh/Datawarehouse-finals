import json
import requests
import pandas as pd
import psycopg2
from io import StringIO

# 🔑 Replace this with the EXACT Raw URL for user_data.json from GitHub
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Customer%20Management%20Department/user_data.json"
)


def main():
    # 1) Download JSON from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()  # raise if 404/500

    # 2) Parse JSON (column-oriented: {column: {idx: value}})
    raw = json.loads(resp.text)
    cols = {col: pd.Series(mapping) for col, mapping in raw.items()}
    df = pd.DataFrame(cols)

    # Parse dates so Postgres can store proper timestamps/dates
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
    if "birthdate" in df.columns:
        df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")

    required_cols = [
        "user_id",
        "creation_date",
        "name",
        "street",
        "state",
        "city",
        "country",
        "birthdate",
        "gender",
        "device_address",
        "user_type",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in JSON: {missing}")

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
        CREATE TABLE IF NOT EXISTS stg_user_data (
            user_id        TEXT,
            creation_date  TIMESTAMP,
            name           TEXT,
            street         TEXT,
            state          TEXT,
            city           TEXT,
            country        TEXT,
            birthdate      DATE,
            gender         TEXT,
            device_address TEXT,
            user_type      TEXT
        );
        TRUNCATE TABLE stg_user_data;
    """)

    # 5) Bulk insert using COPY
    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_user_data (
            user_id,
            creation_date,
            name,
            street,
            state,
            city,
            country,
            birthdate,
            gender,
            device_address,
            user_type
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