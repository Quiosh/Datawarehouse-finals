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
    except Exception:
        try:
            df = pd.read_csv(BytesIO(file_bytes))
        except Exception as e2:
            raise ValueError(f"Failed to read file as Excel or CSV: {e2}")

    # ==========================================
    # 🧹 DATA CLEANING STEPS
    # ==========================================

    # 1. Standardize Headers
    #    "Product_id" -> "product_id"
    df.columns = df.columns.str.lower().str.strip()

    # 2. Drop Junk Columns
    df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]

    # 3. Clean Product Type (Standardization)
    #    - Convert to lowercase
    #    - Replace underscores with spaces ("readymade_breakfast" -> "readymade breakfast")
    #    - Strip whitespace
    if "product_type" in df.columns:
        df["product_type"] = df["product_type"].astype(str).str.lower().str.replace('_', ' ').str.strip()

    # 4. Clean Product Name (NEW STEP)
    #    - Convert to lowercase (Standardizes "Wok" and "wok")
    #    - Strip whitespace
    if "product_name" in df.columns:
        df["product_name"] = df["product_name"].astype(str).str.lower().str.strip()

    # 5. Clean Price
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        
        # Filter out invalid prices (NaN after coercion)
        # Assuming price is required.
        invalid_price_rows = df["price"].isna()
        if invalid_price_rows.any():
            print(f"Warning: Dropping {invalid_price_rows.sum()} rows with invalid or missing 'price'.")
            df = df[~invalid_price_rows]

    # 6. Safety Deduplication
    df = df.drop_duplicates()

    # ==========================================

    # Verify Columns
    required_cols = ["product_id", "product_name", "product_type", "price"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(
            f"Missing expected columns in file: {missing}. "
            f"Available columns: {list(df.columns)}"
        )

    # 7. Filter out rows with missing critical IDs (product_id)
    missing_id_mask = df["product_id"].isna() | (df["product_id"] == "")
    if missing_id_mask.any():
        dropped_count = missing_id_mask.sum()
        print(f"Warning: Dropping {dropped_count} rows with missing 'product_id'.")
        df = df[~missing_id_mask]

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
        "columns_found": list(df.columns)
    }