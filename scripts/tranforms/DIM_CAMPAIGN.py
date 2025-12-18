import psycopg2
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def main():
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    try:
        logging.info("Starting DIM_CAMPAIGN processing...")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_campaign (
                campaign_key BIGSERIAL PRIMARY KEY,
                campaign_id TEXT UNIQUE NOT NULL,
                campaign_name TEXT,
                description TEXT,
                discount NUMERIC
            );
        """)

        # --- REFRESH STEP: Clear the table first ---
        logging.info("Truncating DIM_CAMPAIGN table...")
        cur.execute("TRUNCATE TABLE dim_campaign CASCADE;")
        # -------------------------------------------

        logging.info("Loading campaigns...")
        # Since table is empty, we don't need 'WHERE NOT EXISTS' anymore
        cur.execute("""
            INSERT INTO dim_campaign (campaign_id, campaign_name, description, discount)
            SELECT DISTINCT
                campaign_id,
                campaign_name,
                campaign_description,
                discount
            FROM stg_campaign_data;
        """)

        inserted_count = cur.rowcount
        conn.commit()
        logging.info(f"DIM_CAMPAIGN refreshed. Loaded {inserted_count} rows.")

    except Exception as e:
        conn.rollback()
        logging.error(f"DIM_CAMPAIGN failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()