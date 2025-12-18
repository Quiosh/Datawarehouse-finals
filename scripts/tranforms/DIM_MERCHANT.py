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
        logging.info("Starting DIM_MERCHANT enrichment...")

        drop_cols_query = """
            ALTER TABLE dim_merchant 
            DROP COLUMN IF EXISTS valid_from,
            DROP COLUMN IF EXISTS valid_to,
            DROP COLUMN IF EXISTS is_current;
        """
        cur.execute(drop_cols_query)

        columns_to_add = [
            "city TEXT",
            "state TEXT",
            "country TEXT",
        ]
        for col in columns_to_add:
            cur.execute(f"ALTER TABLE dim_merchant ADD COLUMN IF NOT EXISTS {col};")

        logging.info("Updating Merchant Locations...")
        cur.execute("""
            UPDATE dim_merchant d
            SET 
                city = s.city,
                state = s.state,
                country = s.country
            FROM stg_merchant_data s
            WHERE d.source_merchant_id = s.merchant_id;
        """)

        conn.commit()
        logging.info(" DIM_MERCHANT enrichment complete.")

    except Exception as e:
        conn.rollback()
        logging.error(f" DIM_MERCHANT failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
