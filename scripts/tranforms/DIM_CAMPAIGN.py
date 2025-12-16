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