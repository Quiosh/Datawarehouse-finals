import io
import requests
import pandas as pd
import psycopg2
from io import StringIO

URL_TEST_FILE = "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/datasets/Test%20Files/user_job.csv"

def main(file_bytes: bytes = None):
    print("Starting Test Data Injection: User Job")

    if file_bytes:
        print("Processing Uploaded File...")
        df = pd.read_csv(io.BytesIO(file_bytes))
    else:
        print(f"Fetching from GitHub: {URL_TEST_FILE}")
        resp = requests.get(URL_TEST_FILE, timeout=30)
        df = pd.read_csv(StringIO(resp.text))

    df.columns = df.columns.str.lower().str.strip()
    
    required_cols = ["user_id", "name", "job_title", "job_level"]
    for c in required_cols:
        if c not in df.columns: df[c] = None

    # Add duplicate flags
    df["possible_duplicate"] = False
    df["possible_duplicate_of"] = None

    conn = psycopg2.connect(host="db", port=5432, user="postgres", password="shopzada", dbname="shopzada")
    cur = conn.cursor()

    final_cols = required_cols + ["possible_duplicate", "possible_duplicate_of"]

    buffer = StringIO()
    df[final_cols].to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    print(f"Appending {len(df)} rows to stg_user_job...")
    cur.copy_expert(f"COPY stg_user_job ({','.join(final_cols)}) FROM STDIN WITH (FORMAT csv)", buffer)
    
    conn.commit()
    conn.close()
    return {"rows_injected": len(df)}