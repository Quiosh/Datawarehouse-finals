import requests
import pandas as pd
import psycopg2
from io import BytesIO, StringIO
import openpyxl  # Required for pandas to read Excel files

# 🔑 Replace with the EXACT Raw URL for product_list.xlsx from GitHub
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Business%20Department/product_list.xlsx"
)


def main():
    # 1) Download file from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()
    file_bytes = resp.content

    # 2) Try reading as Excel, fall back to CSV if needed
    try:
        df = pd.read_excel(BytesIO(file_bytes), engine="openpyxl")
    except Exception as e:
        try:
            df = pd.read_csv(BytesIO(file_bytes))
        except Exception as e2:
            raise ValueError(
                f"Failed to read file as Excel or CSV. "
                f"Excel error: {e}, CSV error: {e2}"
            )

    # Drop junk index column
    if "Unnamed: 0" in df.columns:
        df = df.drop(columns=["Unnamed: 0"])

    # Expected columns from your file
    required_cols = ["product_id", "product_name", "product_type", "price"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing expected columns in file: {missing}. "
            f"Available columns: {list(df.columns)}"
        )

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
        CREATE TABLE IF NOT EXISTS stg_product_list (
            product_id    TEXT,
            product_name  TEXT,
            product_type  TEXT,
            price         NUMERIC
        );
        TRUNCATE TABLE stg_product_list;
    """)

    # 5) Bulk insert using COPY
    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_product_list (product_id, product_name, product_type, price)
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