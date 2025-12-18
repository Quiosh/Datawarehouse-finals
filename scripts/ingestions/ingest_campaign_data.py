import io
import re
import requests
import pandas as pd
import psycopg2
from io import StringIO

# 🔗 RAW URL for Historical Data (The "Messy" Tab-Separated File)
HISTORICAL_FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Marketing%20Department/campaign_data.csv"
)


# ---------- helpers ----------

def _get(url: str) -> requests.Response:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    return resp

def _standardize_campaign_df(df: pd.DataFrame, source_type: str = 'clean') -> pd.DataFrame:
    """
    Unified standardization.
    Args:
        source_type: 'dirty_historical' (for the tab-separated file) or 'clean' (for standard CSV)
    """
    
    # 1. Handle "Dirty" Historical Format
    if source_type == 'dirty_historical':
        # The historical file is tab-separated stuffed into column 0
        df = df.iloc[:, 0].str.split('\t', expand=True)
        
        # Drop the junk index column (Column 0) if it exists
        if df.shape[1] >= 5:
            df = df.drop(columns=[0])
            
        # Manually assign headers since the dirty file doesn't have them in a usable way
        if df.shape[1] == 4:
            df.columns = ["campaign_id", "campaign_name", "campaign_description", "discount"]
            
    else:
        # 2. Handle "Clean" Test/Upload Format
        # Normalize headers
        df = df.rename(columns={
            "Campaign_id": "campaign_id", 
            "Campaign_name": "campaign_name", 
            "Description": "campaign_description", 
            "Discount": "discount",
            # Lowercase fallbacks
            "campaign_id": "campaign_id",
            "campaign_name": "campaign_name",
            "description": "campaign_description",
            "discount": "discount"
        })

    # 3. Ensure Schema & Fill Missing
    required_cols = ["campaign_id", "campaign_name", "campaign_description", "discount"]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None
    df = df[required_cols]

    # 4. Common Cleaning: "discount"
    #    Remove '%' or letters, keep numbers/dots.
    if "discount" in df.columns:
        df["discount"] = df["discount"].astype(str).str.replace(r'[^0-9.]', '', regex=True)
        df["discount"] = pd.to_numeric(df["discount"], errors='coerce')

    # 5. Common Cleaning: "campaign_description"
    #    Remove excessive quotes often found in raw files
    if "campaign_description" in df.columns:
        df["campaign_description"] = df["campaign_description"].astype(str).str.replace('"', '')

    return df


# ---------- main ----------

def main(new_campaign_file: bytes = None):
    # ==========================================
    # PART 1: LOAD HISTORICAL DATA
    # ==========================================
    print("⏳ Loading historical campaign data...")
    
    # Download raw text
    resp = _get(HISTORICAL_FILE_URL)
    # Parse as 'dirty_historical' because we know this specific URL is the messy one
    df_historical = _standardize_campaign_df(
        pd.read_csv(io.StringIO(resp.text)), 
        source_type='dirty_historical'
    )

    # Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    table_name = "stg_campaign_data"

    # Drop & Recreate Staging Table (Full Refresh for Historical)
    print(f"🗑️ Recreating table {table_name}...")
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    
    cur.execute(f"""
        CREATE TABLE {table_name} (
            campaign_id          TEXT,
            campaign_name        TEXT,
            campaign_description TEXT,
            discount             NUMERIC
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
            campaign_id, 
            campaign_name, 
            campaign_description, 
            discount
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
    df_new_campaigns = pd.DataFrame()

    # 1) Load Data (Upload OR URL Fallback)
    if new_campaign_file:
        print("📥 Processing manually uploaded campaign file...")
        try:
            file_stream = io.BytesIO(new_campaign_file)
            # We assume uploads are standard CSVs ('clean')
            df_new_campaigns = _standardize_campaign_df(pd.read_csv(file_stream), source_type='clean')
            print(f"✅ Successfully loaded {len(df_new_campaigns)} rows from upload.")
        except Exception as e:
            print(f"❌ Error reading uploaded file: {e}")
            raise e
    else:
        # Fallback to URL
        print(f"🌐 No upload provided. Checking default test file URL...")
        try:
            resp = _get(URL_LATE_CAMPAIGN_FILE)
            # We assume the test file is a standard CSV ('clean')
            df_new_campaigns = _standardize_campaign_df(pd.read_csv(StringIO(resp.text)), source_type='clean')
            print(f"✅ Successfully loaded {len(df_new_campaigns)} rows from URL.")
        except Exception as e:
            print(f"⚠️ Could not load from URL or no test data found: {e}")
            pass

    if not df_new_campaigns.empty:
        # 2) Bulk Insert (APPEND ONLY)
        buffer = StringIO()
        df_new_campaigns.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        try:
            cur.copy_expert(
                f"""
                COPY {table_name} (
                    campaign_id, 
                    campaign_name, 
                    campaign_description, 
                    discount
                )
                FROM STDIN WITH (FORMAT csv)
                """,
                buffer,
            )
            conn.commit()
            print(f"➕ Appended {len(df_new_campaigns)} new rows to {table_name}.")
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Database error during append: {e}")
            raise e
    
    cur.close()
    conn.close()

    return {
        "table": table_name,
        "historical_rows": len(df_historical),
        "test_rows_appended": len(df_new_campaigns),
        "total_rows": len(df_historical) + len(df_new_campaigns)
    }


if __name__ == "__main__":
    main()