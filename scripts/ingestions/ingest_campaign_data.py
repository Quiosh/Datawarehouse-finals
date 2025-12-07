import io
import re
import requests
import pandas as pd
import psycopg2
from io import StringIO

# 🔑 Raw URL (This file is "dirty" and needs the cleaning logic below)
FILE_URL = "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/datasets/Marketing%20Department/campaign_data.csv"


def main():
    # 1) Download CSV from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()

    # 2) Read Raw File
    #    The GitHub file is technically a CSV, but all the data is stuffed 
    #    into the first column, separated by tabs.
    df_raw = pd.read_csv(io.StringIO(resp.text))

    # ==========================================
    # 🧹 DATA CLEANING STEPS
    # ==========================================

    # 1. Extract Hidden Data
    #    Split the first column by Tab ('\t') to get the real data columns.
    #    This creates columns: [Index, ID, Name, Description, Discount]
    df = df_raw.iloc[:, 0].str.split('\t', expand=True)

    # 2. Drop Junk Index Column (Column 0)
    #    The first extracted column is just row numbers (0, 1, 2...). We remove it.
    if df.shape[1] >= 5:
        df = df.drop(columns=[0])

    # 3. Assign Correct Headers
    df.columns = ["campaign_id", "campaign_name", "campaign_description", "discount"]

    # 4. Clean "discount" Column
    #    Converts '1%', '10%%', '1pct' -> 1, 10, 1
    #    Regex removes anything that is NOT a digit or dot.
    df["discount"] = df["discount"].astype(str).str.replace(r'[^0-9.]', '', regex=True)
    #    Safely convert to number (coercing errors to NaN)
    df["discount"] = pd.to_numeric(df["discount"], errors='coerce')

    # 5. Clean "campaign_description"
    #    Removes the excessive triple-quotes ("""Text""") found in the raw data.
    df["campaign_description"] = df["campaign_description"].astype(str).str.replace('"', '')

    # ==========================================

    # 4) Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    # 5) Recreate Staging Table
    cur.execute("DROP TABLE IF EXISTS stg_campaign_data;")
    
    cur.execute("""
        CREATE TABLE stg_campaign_data (
            campaign_id           TEXT,
            campaign_name         TEXT,
            campaign_description  TEXT,
            discount              NUMERIC
        );
    """)

    # 6) Bulk insert using COPY
    buffer = StringIO()
    # Write to buffer as standard CSV (comma separated)
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)

    cur.copy_expert(
        """
        COPY stg_campaign_data (
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
    cur.close()
    conn.close()

    return {
        "rows_loaded": len(df),
        "source_url": FILE_URL,
        "columns_cleaned": list(df.columns)
    }