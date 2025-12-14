import psycopg2
import logging

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
        logging.info("Starting DIM_STAFF enrichment...")

        # 2) Remove history columns (Convert to SCD Type 1)
        drop_cols_query = """
            ALTER TABLE dim_staff 
            DROP COLUMN IF EXISTS is_current,
            DROP COLUMN IF EXISTS valid_from,
            DROP COLUMN IF EXISTS valid_to;
        """
        cur.execute(drop_cols_query)

        # 3) Add Missing Columns
        columns_to_add = [
            "job_level TEXT",
            "city TEXT",
            "country TEXT",
        ]
        for col in columns_to_add:
            cur.execute(f"ALTER TABLE dim_staff ADD COLUMN IF NOT EXISTS {col};")

        # 4) Update Details (From stg_staff_data)
        logging.info("Updating Staff Details...")
        cur.execute("""
            UPDATE dim_staff d
            SET 
                job_level = s.job_level,
                city = s.city,
                country = s.country
            FROM stg_staff_data s
            WHERE d.source_staff_id = s.staff_id;
        """)

        conn.commit()
        logging.info(" DIM_STAFF enrichment complete.")

    except Exception as e:
        conn.rollback()
        logging.error(f" DIM_STAFF failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()