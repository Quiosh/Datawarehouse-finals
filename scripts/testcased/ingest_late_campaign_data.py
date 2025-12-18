import io
import requests
import pandas as pd
import psycopg2
from io import StringIO

URL_LATE_CAMPAIGN_FILE = (
    "https://raw.githubusercontent.com/Quiosh/dwh_finalproject_3cse_group_4/main/"
    "datasets/Test%20Files/late_campaign.csv"
)
# ---------- helpers to download + load ----------


def _get(url: str) -> requests.Response:
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp


def _load_csv_from_url(url: str) -> pd.DataFrame:
    """Load CSV from a URL."""
    resp = _get(url)
    return pd.read_csv(StringIO(resp.text))


# ---------- standardization ----------


def _standardize_campaign_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize columns to: campaign_id, campaign_name, campaign_description, discount
    """
    # 1. Normalize Headers
    # Map typical CSV headers to database columns
    df = df.rename(
        columns={
            "Campaign_id": "campaign_id",
            "Campaign_name": "campaign_name",
            "Description": "campaign_description",
            "Discount": "discount",
            # Lowercase fallbacks
            "campaign_id": "campaign_id",
            "campaign_name": "campaign_name",
            "description": "campaign_description",
            "discount": "discount",
        }
    )

    # 2. Ensure only required columns exist
    required_cols = ["campaign_id", "campaign_name", "campaign_description", "discount"]

    # Fill missing columns with None/Empty to prevent errors
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df = df[required_cols]

    # 3. Clean "discount" Column
    #    Remove '%' or letters, keep numbers/dots.
    if "discount" in df.columns:
        df["discount"] = (
            df["discount"].astype(str).str.replace(r"[^0-9.]", "", regex=True)
        )
        df["discount"] = pd.to_numeric(df["discount"], errors="coerce")

    # 4. Clean "campaign_description"
    #    Remove excessive quotes often found in raw files
    if "campaign_description" in df.columns:
        df["campaign_description"] = (
            df["campaign_description"].astype(str).str.replace('"', "")
        )

    return df


# ---------- main ----------


def main(new_campaign_file: bytes = None):
    """
    Ingest ONLY the late campaign data and APPEND it to the existing staging table.

    Args:
        new_campaign_file: File upload from Windmill (passed as bytes).
    """

    df_new_campaigns = pd.DataFrame()

    # 1) Load Data (Upload OR URL Fallback)
    if new_campaign_file:
        print("📥 Processing manually uploaded campaign file...")
        try:
            # Wrap bytes in BytesIO so pandas can read it
            file_stream = io.BytesIO(new_campaign_file)
            df_new_campaigns = _standardize_campaign_df(pd.read_csv(file_stream))
            print(f"Successfully loaded {len(df_new_campaigns)} rows from upload.")
        except Exception as e:
            print(f"Error reading uploaded file: {e}")
            raise e
    else:
        # Fallback to URL
        print(
            f"🌐 No upload provided. Attempting to load from URL: {URL_LATE_CAMPAIGN_FILE}"
        )
        try:
            df_new_campaigns = _standardize_campaign_df(
                _load_csv_from_url(URL_LATE_CAMPAIGN_FILE)
            )
            print(f"✅ Successfully loaded {len(df_new_campaigns)} rows from URL.")
        except Exception as e:
            print(f"⚠️ Could not load from URL (check if placeholder is updated): {e}")
            # We allow the script to pass even if no data is found (returns 0 rows)
            pass

    # If nothing loaded, stop here
    if df_new_campaigns.empty:
        print("⚠️ No new campaign data to append.")
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

    table_name = "stg_campaign_data"

    # 3) Bulk Insert (APPEND ONLY)
    # We use COPY for speed. We do NOT drop the table.
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
        print(f" Successfully appended {len(df_new_campaigns)} rows to {table_name}.")

    except Exception as e:
        conn.rollback()
        print(f" Database error: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

    return {
        "table": table_name,
        "rows_appended": len(df_new_campaigns),
        "status": "Success",
    }


if __name__ == "__main__":
    main()
