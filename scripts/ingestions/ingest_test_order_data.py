import io
import requests
import pandas as pd
import psycopg2
from io import StringIO

URL_TEST_FILE = "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/datasets/Test%20Files/order_data.csv"

def main(file_bytes: bytes = None):
    print("Starting Test Data Injection: Order Data")

    if file_bytes:
        print("Processing Uploaded File...")
        df = pd.read_csv(io.BytesIO(file_bytes))
    else:
        print(f"Fetching from GitHub: {URL_TEST_FILE}")
        resp = requests.get(URL_TEST_FILE, timeout=30)
        df = pd.read_csv(StringIO(resp.text))

    rename_map = {}
    for c in df.columns:
        lc = str(c).strip().lower().replace(" ", "_")
        if "estimated" in lc: rename_map[c] = "estimated_arrival"
        elif "order" in lc and "id" in lc: rename_map[c] = "order_id"
        elif "user" in lc and "id" in lc: rename_map[c] = "user_id"
        elif "trans" in lc: rename_map[c] = "transaction_date"
    
    df = df.rename(columns=rename_map)
    required_cols = ["order_id", "user_id", "estimated_arrival", "transaction_date"]
    
    for r in required_cols:
        if r not in df.columns: df[r] = None
    
    # Cleaning
    df["estimated_arrival"] = df["estimated_arrival"].astype(str).str.replace(r"\D", "", regex=True)
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce")

    conn = psycopg2.connect(host="db", port=5432, user="postgres", password="shopzada", dbname="shopzada")
    cur = conn.cursor()

    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    print(f"📥 Appending {len(df)} rows to stg_order_data...")
    cur.copy_expert(f"COPY stg_order_data ({','.join(required_cols)}) FROM STDIN WITH (FORMAT csv)", buffer)
    
    conn.commit()
    conn.close()
    return {"rows_injected": len(df)}