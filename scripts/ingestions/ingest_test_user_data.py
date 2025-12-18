import io
import requests
import pandas as pd
import psycopg2
from io import StringIO

# 🔗 GitHub URL for the TEST file
URL_TEST_FILE = "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/datasets/Test%20Files/user_data.csv"

def main(file_bytes: bytes = None):
    print(" Starting Test Data Injection: User Data")
    
    # 1. Get Data (Upload OR GitHub)
    if file_bytes:
        print(" Processing Uploaded File...")
        df = pd.read_csv(io.BytesIO(file_bytes))
    else:
        print(f" Fetching Default Test File from GitHub: {URL_TEST_FILE}")
        try:
            resp = requests.get(URL_TEST_FILE, timeout=30)
            resp.raise_for_status()
            df = pd.read_csv(StringIO(resp.text))
        except Exception as e:
            print(f" Could not fetch test file: {e}")
            return {"status": "skipped", "reason": "No upload and GitHub fetch failed"}

    # 2. Standardize Columns
    df.columns = df.columns.str.lower().str.strip()
    
    # Ensure all Staging Columns exist
    required_cols = ["user_id", "creation_date", "name", "street", "state", "city", 
                     "country", "birthdate", "gender", "device_address", "user_type"]
    
    # Fill missing with None
    for c in required_cols:
        if c not in df.columns: df[c] = None

    # Add duplicate flags (default to False for test data)
    df["possible_duplicate"] = False
    df["possible_duplicate_of"] = None

    # Date Cleaning
    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
    if "birthdate" in df.columns:
        df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")

    # 3. Append to DB
    conn = psycopg2.connect(host="db", port=5432, user="postgres", password="shopzada", dbname="shopzada")
    cur = conn.cursor()
    
    # We define the column order explicitly for COPY
    final_cols = required_cols + ["possible_duplicate", "possible_duplicate_of"]
    
    buffer = StringIO()
    df[final_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    
    print(f" Appending {len(df)} rows to stg_user_data...")
    try:
        cur.copy_expert(f"COPY stg_user_data ({','.join(final_cols)}) FROM STDIN WITH (FORMAT csv)", buffer)
        conn.commit()
        print(" Success.")
    except Exception as e:
        print(f" Error appending data: {e}")
        raise e
    finally:
        conn.close()

    return {"rows_injected": len(df)}