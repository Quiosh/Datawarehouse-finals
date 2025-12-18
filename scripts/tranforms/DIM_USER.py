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
        logging.info("Starting DIM_USER enrichment...")

        # Remove columns not present in schema
        drop_cols_query = """
            ALTER TABLE dim_user 
            DROP COLUMN IF EXISTS creation_date,
            DROP COLUMN IF EXISTS valid_from,
            DROP COLUMN IF EXISTS valid_to,
            DROP COLUMN IF EXISTS is_current;
        """
        cur.execute(drop_cols_query)

        # 2) Add Missing Columns
        columns_to_add = [
            "birthdate DATE",
            "gender TEXT",
            "user_type TEXT",
            "city TEXT",
            "state TEXT",
            "country TEXT",
            "job_title TEXT",
            "job_level TEXT",
        ]
        for col in columns_to_add:
            cur.execute(f"ALTER TABLE dim_user ADD COLUMN IF NOT EXISTS {col};")

        # 3) Update Demographics (From stg_user_data)
        logging.info("Updating User Demographics...")
        cur.execute("""
            UPDATE dim_user d
            SET 
                birthdate = s.birthdate,
                gender = s.gender,
                user_type = s.user_type,
                city = s.city,
                state = s.state,
                country = s.country
            FROM stg_user_data s
            WHERE d.source_user_id = s.user_id;
        """)

        # 4) Update Jobs (From stg_user_job)
        logging.info("Updating User Jobs...")
        cur.execute("""
            UPDATE dim_user d
            SET 
                job_title = j.job_title,
                job_level = j.job_level
            FROM stg_user_job j
            WHERE d.source_user_id = j.user_id;
        """)

        conn.commit()
        logging.info(" DIM_USER enrichment complete.")

    except Exception as e:
        conn.rollback()
        logging.error(f" DIM_USER failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()
