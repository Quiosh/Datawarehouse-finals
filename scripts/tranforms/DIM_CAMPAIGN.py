import psycopg2
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main():
    # 1) Connect to Postgres
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

        # 2) Create Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_campaign (
                campaign_key BIGSERIAL PRIMARY KEY,
                campaign_id TEXT UNIQUE NOT NULL,
                campaign_name TEXT,
                description TEXT,
                discount NUMERIC
            );
        """)

        # 3) Extract & Load (Insert new records only)
        # We perform an INSERT INTO ... SELECT ... WHERE NOT EXISTS to handle duplicates efficiently
        logging.info("Loading new campaigns...")
        cur.execute("""
            INSERT INTO dim_campaign (campaign_id, campaign_name, description, discount)
            SELECT DISTINCT
                campaign_id,
                campaign_name,
                campaign_description,
                discount
            FROM stg_campaign_data s
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_campaign d WHERE d.campaign_id = s.campaign_id
            );
        """)

        # Log count
        inserted_count = cur.rowcount
        conn.commit()
        logging.info(f" DIM_CAMPAIGN loaded. Inserted {inserted_count} new rows.")

    except Exception as e:
        conn.rollback()
        logging.error(f" DIM_CAMPAIGN failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()