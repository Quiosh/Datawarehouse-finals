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
        logging.info("Starting DIM_PRODUCT processing...")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_product (
                product_key BIGSERIAL PRIMARY KEY,
                product_id TEXT NOT NULL,
                product_name TEXT,
                product_type TEXT,
                base_price NUMERIC
            );
        """)

        logging.info("Loading products...")
        cur.execute("""
            INSERT INTO dim_product (product_id, product_name, product_type, base_price)
            SELECT DISTINCT
                product_id,
                product_name,
                product_type,
                price
            FROM stg_product_list s
            WHERE NOT EXISTS (
                SELECT 1 FROM dim_product d WHERE d.product_id = s.product_id
            );
        """)

        count = cur.rowcount
        conn.commit()
        logging.info(f" DIM_PRODUCT loaded. Inserted {count} new products.")

    except Exception as e:
        conn.rollback()
        logging.error(f" DIM_PRODUCT failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()