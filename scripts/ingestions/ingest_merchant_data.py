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


def _parse_html_table(html_str: str) -> pd.DataFrame:
    """
    Try pandas.read_html first (with lxml). If that fails, fall back
    to the manual regex-based table parser you already had.
    """
    # 1) Try pandas.read_html (usually enough)
    try:
        tables = pd.read_html(html_str)
        if tables:
            return tables[0]
    except Exception:
        pass  # fall back to manual parsing

    # 2) Manual regex-based parsing (your original logic)
    table_match = re.search(
        r"<table[^>]*>(.*?)</table>", html_str, re.DOTALL | re.IGNORECASE
    )
    if not table_match:
        raise ValueError("No tables found in HTML file")

    table_content = table_match.group(1)
    rows = re.findall(
        r"<tr[^>]*>(.*?)</tr>", table_content, re.DOTALL | re.IGNORECASE
    )
    if not rows:
        raise ValueError("No rows found in HTML table")

    data = []
    headers = None

    for i, row in enumerate(rows):
        cells = re.findall(
            r"<t[dh][^>]*>(.*?)</t[dh]>", row, re.DOTALL | re.IGNORECASE
        )
        clean_cells = [re.sub(r"<[^>]+>", "", cell).strip() for cell in cells]

        if i == 0:
            headers = clean_cells
        else:
            data.append(clean_cells)

    if not (headers and data):
        raise ValueError("Could not parse table data")

    return pd.DataFrame(data, columns=headers)


def main():
    # 1) Download HTML from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()
    html_str = resp.text

    # 2) Parse the table into a DataFrame
    df = _parse_html_table(html_str)

    # Drop junk index column if present
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Expected columns (from merchant_data.html)
    required_cols = [
        "merchant_id",
        "creation_date",
        "name",
        "street",
        "state",
        "city",
        "country",
        "contact_number",
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
    }