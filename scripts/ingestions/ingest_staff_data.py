import io
import requests
import pandas as pd
import psycopg2
from io import StringIO
import lxml  # ensure lxml is installed for read_html


# 🔑 Replace with the EXACT Raw URL of staff_data.html from GitHub
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Enterprise%20Department/staff_data.html"
)


def main():
    # 1) Download HTML from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()  # raises if 404/500

    html_str = resp.text

    # 2) Parse first table in the HTML
    tables = pd.read_html(html_str)  # uses lxml under the hood
    if not tables:
        raise ValueError("No tables found in staff_data HTML")

    df = tables[0]

    # Drop junk index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Expected columns from staff_data.html
    required_cols = [
        "staff_id",
        "name",
        "job_level",
        "street",
        "state",
        "city",
        "country",
        "contact_number",
        "creation_date",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in HTML: {missing}")

    # Parse creation_date to timestamp
    df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")

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

    # 5) Bulk insert using COPY
    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_staff_data (
            staff_id,
            name,
            job_level,
            street,
            state,
            city,
            country,
            contact_number,
            creation_date
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