import io
import re
import requests
import pandas as pd
import psycopg2
from io import StringIO

# 🔗 RAW URL for Historical Data
HISTORICAL_FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Marketing%20Department/transactional_campaign_data.csv"
)


# ---------- helpers ----------

def _get(url: str) -> requests.Response:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp

def _standardize_links_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Unified standardization for both historical and test data.
    Enforces schema: transaction_date, campaign_id, order_id, estimated_arrival, availed
    """
    # 1. Drop junk columns like "Unnamed: 0"
    df = df.loc[:, ~df.columns.str.contains("^Unnamed:", case=False)]

    # 2. Normalize Headers
    #    Map the CSV headers (often Title Case) to DB columns (lowercase)
    rename_map = {}
    for col in df.columns:
        lc = str(col).strip().lower()
        if lc == "campaign_id":
            rename_map[col] = "campaign_id"
        elif lc == "order_id":
            rename_map[col] = "order_id"
        elif lc == "transaction_date":
            rename_map[col] = "transaction_date"
        elif lc.replace(" ", "_") == "estimated_arrival":
            rename_map[col] = "estimated_arrival"
        elif lc == "availed":
            rename_map[col] = "availed"

    df = df.rename(columns=rename_map)

    # 3. Ensure ALL Target Columns Exist
    required_cols = ["transaction_date", "campaign_id", "order_id", "estimated_arrival", "availed"]
    
    for col in required_cols:
        if col not in df.columns:
            # Fill missing columns with None (NULL in Postgres)
            df[col] = None

    # 4. Clean specific columns
    if "estimated_arrival" in df.columns:
        # Remove "days" text, keep numbers
        df["estimated_arrival"] = df["estimated_arrival"].astype(str).str.replace(r'\D', '', regex=True)
        df["estimated_arrival"] = pd.to_numeric(df["estimated_arrival"], errors="coerce")

    if "transaction_date" in df.columns:
        # Ensure it is a valid date string
        # Coerce errors to NaT, then drop NaT if necessary, or keep as None
        df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    if "availed" in df.columns:
        # Ensure 1/0 integer
        df["availed"] = pd.to_numeric(df["availed"], errors="coerce").fillna(0).astype(int)

    # 5. Clean IDs (Strip whitespace)
    if "order_id" in df.columns:
        df["order_id"] = df["order_id"].astype(str).str.strip()
    if "campaign_id" in df.columns:
        df["campaign_id"] = df["campaign_id"].astype(str).str.strip()

    # Return only the columns needed, in the correct order
    return df[required_cols]


# ---------- main ----------

def main(new_links_file: bytes = None):
    # ==========================================
    # PART 1: LOAD HISTORICAL DATA
    # ==========================================
    print("⏳ Loading historical transactional campaign data...")
    
    # Download and Standardize
    resp = _get(HISTORICAL_FILE_URL)
    df_historical = _standardize_links_df(pd.read_csv(io.StringIO(resp.text)))

    # Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    table_name = "stg_transactional_campaign_data"

    # Drop & Recreate Staging Table (Full Refresh for Historical)
    print(f"🗑️ Recreating table {table_name}...")
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    
    # We create the table with specific types matching our cleaning logic
    cur.execute(f"""
        CREATE TABLE {table_name} (
            transaction_date   TIMESTAMP,
            campaign_id        TEXT,
            order_id           TEXT,
            estimated_arrival  INTEGER,
            availed            INTEGER
        );
    """)

    # Bulk insert Historical Data
    print(f"📥 Inserting {len(df_historical)} historical rows...")
    buffer = StringIO()
    df_historical.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        f"""
        COPY {table_name} (
            transaction_date, 
            campaign_id, 
            order_id, 
            estimated_arrival, 
            availed
        )
        FROM STDIN WITH (FORMAT csv)
        """,
        buffer,
    )
    conn.commit()

    # ==========================================
    # PART 2: LOAD NEW TEST DATA (APPEND MODE)
    # ==========================================
    print("🔎 Checking for new test data to append...")
    df_new_links = pd.DataFrame()

    # 1) Load Data (Upload OR URL Fallback)
    if new_links_file:
        print("📥 Processing uploaded links file...")
        try:
            file_stream = io.BytesIO(new_links_file)
            df_new_links = _standardize_links_df(pd.read_csv(file_stream))
            print(f"✅ Successfully loaded {len(df_new_links)} rows from upload.")
        except Exception as e:
            print(f"❌ Error reading uploaded file: {e}")
            raise e
    else:
        # Fallback to URL
        print(f"🌐 No upload provided. Checking default test file URL...")
        try:
            resp = _get(URL_LATE_LINKS_FILE)
            df_new_links = _standardize_links_df(pd.read_csv(StringIO(resp.text)))
            print(f"✅ Successfully loaded {len(df_new_links)} rows from URL.")
        except Exception as e:
            print(f"⚠️ Could not load from URL or no test data found: {e}")
            pass

    if not df_new_links.empty:
        # 2) Bulk Insert (APPEND ONLY)
        buffer = StringIO()
        df_new_links.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        try:
            cur.copy_expert(
                f"""
                COPY {table_name} (
                    transaction_date, 
                    campaign_id, 
                    order_id, 
                    estimated_arrival, 
                    availed
                )
                FROM STDIN WITH (FORMAT csv)
                """,
                buffer,
            )
            conn.commit()
            print(f"➕ Appended {len(df_new_links)} new rows to {table_name}.")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Database error during append: {e}")
            raise e
    
    cur.close()
    conn.close()

    return {
        "table": table_name,
        "historical_rows": len(df_historical),
        "test_rows_appended": len(df_new_links),
        "total_rows": len(df_historical) + len(df_new_links)
    }


if __name__ == "__main__":
    main()