import io
import re
import requests
import pandas as pd
import psycopg2
from io import StringIO

# 🔑 Raw URL from GitHub
FILE_URL = (
    "https://raw.githubusercontent.com/Quiosh/Datawarehouse-finals/main/"
    "datasets/Marketing%20Department/campaign_data.csv"
)


def _parse_discount(value):
    """
    Extract the numeric part of discount strings like:
      '5%', '5 percent', '5pct', '10 PERCENT', etc.
    Returns the number (e.g., 5.0) or None if no digits found.
    """
    if value is None:
        return None
    s = str(value).lower().strip()
    m = re.search(r"(\d+(\.\d+)?)", s)
    if not m:
        return None
    return float(m.group(1))


def main():
    # 1) Download file from GitHub
    resp = requests.get(FILE_URL, timeout=30)
    resp.raise_for_status()

    # 2) Read as TSV (tab-separated), NOT comma-separated
    df_raw = pd.read_csv(
        io.StringIO(resp.text),
        sep="\t",
        engine="python"
    )

    # Drop junk index columns like "Unnamed: 0"
    junk_cols = [c for c in df_raw.columns if str(c).lower().startswith("unnamed")]
    if junk_cols:
        df_raw = df_raw.drop(columns=junk_cols)

    # Must have at least 4 columns now
    if df_raw.shape[1] < 4:
        raise ValueError(
            f"Expected at least 4 columns in campaign_data.csv (TSV), "
            f"got {df_raw.shape[1]}: {list(df_raw.columns)}"
        )

    # 3) Map by position:
    # col0 -> campaign_id
    # col1 -> campaign_name
    # col2 -> campaign_description
    # col3 -> discount (parsed)
    discount_raw = df_raw.iloc[:, 3]

    df = pd.DataFrame({
        "campaign_id":          df_raw.iloc[:, 0].astype(str),
        "campaign_name":        df_raw.iloc[:, 1].astype(str),
        "campaign_description": df_raw.iloc[:, 2].astype(str),
        "discount":             discount_raw.apply(_parse_discount),
    })

    # 4) Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    table_name = "stg_campaign_data"

    # 5) Drop & recreate staging table
    cur.execute(f"DROP TABLE IF EXISTS {table_name};")
    cur.execute(f"""
        CREATE TABLE {table_name} (
            campaign_id            TEXT,
            campaign_name          TEXT,
            campaign_description   TEXT,
            discount               NUMERIC
        );
    """)

    # 6) Bulk insert using COPY
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
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
    cur.close()
    conn.close()

    return {
        "rows_loaded": len(df),
        "columns": ["campaign_id", "campaign_name", "campaign_description", "discount"],
        "source_url": FILE_URL,
    }