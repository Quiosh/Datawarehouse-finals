import json
import requests
import pandas as pd
import psycopg2
import io
from io import StringIO

# 🔑 EXACT Raw URL for user_data.json from GitHub
FILE_URL = "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Customer%20Management%20Department/user_data.json"

def main(file_bytes: bytes = None):
    # ==========================================
    # 📥 LOAD DATA (Hybrid: Upload vs GitHub)
    # ==========================================
    if file_bytes:
        print("📂 Mode: File Upload (Test CSV)")
        df = pd.read_csv(io.BytesIO(file_bytes))
    else:
        print(f"☁️ Mode: GitHub Fetch ({FILE_URL})")
        resp = requests.get(FILE_URL, timeout=30)
        resp.raise_for_status()
        
        # Original JSON Logic
        raw = json.loads(resp.text)
        cols = {col: pd.Series(mapping) for col, mapping in raw.items()}
        df = pd.DataFrame(cols)

    # ==========================================
    # 🧹 CLEANING (Shared Logic)
    # ==========================================
    df.columns = df.columns.str.lower().str.strip()
    df = df.loc[:, ~df.columns.str.contains('^unnamed', case=False)]

    if "creation_date" in df.columns:
        df["creation_date"] = pd.to_datetime(df["creation_date"], errors="coerce")
    if "birthdate" in df.columns:
        df["birthdate"] = pd.to_datetime(df["birthdate"], errors="coerce")

    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # Fix Booleans
    if 'possible_duplicate' in df.columns:
        df['possible_duplicate'] = df['possible_duplicate'].astype(bool)

    # Deduplication Logic
    if 'user_id' in df.columns and 'creation_date' in df.columns:
        df = df.sort_values(by=['user_id', 'creation_date'], ascending=[True, False])

    # ==========================================
    # 💾 DATABASE LOAD
    # ==========================================
    conn = psycopg2.connect(host="db", port=5432, user="postgres", password="shopzada", dbname="shopzada")
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS stg_user_data;")
    cur.execute("""
        CREATE TABLE stg_user_data (
            user_id TEXT, creation_date TIMESTAMP, name TEXT, street TEXT, state TEXT, 
            city TEXT, country TEXT, birthdate DATE, gender TEXT, device_address TEXT, 
            user_type TEXT, possible_duplicate BOOLEAN, possible_duplicate_of TEXT
        );
    """)

    required_cols = ["user_id", "creation_date", "name", "street", "state", "city", "country", 
                     "birthdate", "gender", "device_address", "user_type", "possible_duplicate", "possible_duplicate_of"]
    
    for c in required_cols:
        if c not in df.columns: df[c] = None

    buffer = StringIO()
    df[required_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        "COPY stg_user_data (user_id, creation_date, name, street, state, city, country, birthdate, gender, device_address, user_type, possible_duplicate, possible_duplicate_of) FROM STDIN WITH (FORMAT csv)", 
        buffer
    )

    conn.commit()
    conn.close()
    return {"status": "success", "rows": len(df), "mode": "upload" if file_bytes else "github"}