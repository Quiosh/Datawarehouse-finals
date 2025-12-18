import io
import requests
import pandas as pd
import psycopg2
from io import StringIO

# 🔗 PLACEHOLDER URL (Fallback if no file is uploaded)
URL_LATE_LINKS_FILE = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Test%20Files/late_transactional_campaign.csv"
)
# ---------- helpers ----------


def _get(url: str) -> requests.Response:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp


def _standardize_links_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the incoming CSV to match the STAGING SCHEMA.
    Target Columns: transaction_date, campaign_id, order_id, estimated_arrival, availed
    """
    # 1. Normalize Headers
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

    # 2. Ensure ALL Target Columns Exist
    required_cols = [
        "transaction_date",
        "campaign_id",
        "order_id",
        "estimated_arrival",
        "availed",
    ]

    for col in required_cols:
        if col not in df.columns:
            # Fill missing columns with None (NULL in Postgres)
            df[col] = None

    # 3. Clean specific columns if they exist
    if "estimated_arrival" in df.columns:
        # Remove "days" text, keep numbers
        df["estimated_arrival"] = (
            df["estimated_arrival"].astype(str).str.replace(r"\D", "", regex=True)
        )
        df["estimated_arrival"] = pd.to_numeric(
            df["estimated_arrival"], errors="coerce"
        )

    if "transaction_date" in df.columns:
        # Ensure it is a valid date string
        if not df["transaction_date"].isna().all():
            df["transaction_date"] = pd.to_datetime(
                df["transaction_date"], errors="coerce"
            ).dt.date

    if "availed" in df.columns:
        # Ensure 1/0 integer
        df["availed"] = (
            pd.to_numeric(df["availed"], errors="coerce").fillna(0).astype(int)
        )

    # 4. Clean IDs (Strip whitespace)
    if "order_id" in df.columns:
        df["order_id"] = df["order_id"].astype(str).str.strip()
    if "campaign_id" in df.columns:
        df["campaign_id"] = df["campaign_id"].astype(str).str.strip()

    # Return only the columns needed, in the correct order
    return df[required_cols]


# ---------- main ----------


def main(new_links_file: bytes = None):
    """
    Ingest ONLY the late links data and APPEND it to stg_transactional_campaign_data.

    Args:
        new_links_file: File upload from Windmill (passed as bytes).
    """

    df_new_links = pd.DataFrame()

    # 1) Load Data (Upload OR URL Fallback)
    if new_links_file:
        print("📥 Processing uploaded links file...")
        try:
            file_stream = io.BytesIO(new_links_file)
            df_new_links = _standardize_links_df(pd.read_csv(file_stream))
            print(f"Successfully loaded {len(df_new_links)} rows from upload.")
        except Exception as e:
            print(f"Error reading uploaded file: {e}")
            raise e
    else:
        # Fallback to URL
        print(f"🌐 No upload provided. Attempting to load from URL...")
        try:
            resp = _get(URL_LATE_LINKS_FILE)
            df_new_links = _standardize_links_df(pd.read_csv(StringIO(resp.text)))
            print(f"Successfully loaded {len(df_new_links)} rows from URL.")
        except Exception as e:
            print(f"Could not load from URL: {e}")
            pass

    if df_new_links.empty:
        print("⚠️ No new link data to append.")
        return {"rows_loaded": 0}

    # 2) Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    table_name = "stg_transactional_campaign_data"

    # 3) Bulk Insert (APPEND ONLY)
    buffer = StringIO()
    # Write to buffer
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
        print(f"Successfully appended {len(df_new_links)} rows to {table_name}.")

    except Exception as e:
        conn.rollback()
        print(f"Database error: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

    return {
        "table": table_name,
        "rows_appended": len(df_new_links),
        "status": "Success",
    }


if __name__ == "__main__":
    main()
