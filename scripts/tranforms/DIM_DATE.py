import psycopg2
import logging
from datetime import date, timedelta

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
        logging.info("Starting DIM_DATE processing...")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_date (
                date_key INT PRIMARY KEY,
                full_date DATE UNIQUE,
                year INT,
                quarter INT,
                month INT,
                day INT,
                day_name TEXT,
                month_name TEXT,
                is_weekend BOOLEAN
            );
        """)

        # --- REFRESH STEP: Clear the table first ---
        logging.info("Truncating DIM_DATE table...")
        cur.execute("TRUNCATE TABLE dim_date CASCADE;")
        # -------------------------------------------

        start_date = date(2020, 1, 1)
        end_date = date(2030, 12, 31)
        delta = end_date - start_date

        logging.info("Generating date records...")

        insert_query = """
            INSERT INTO dim_date (date_key, full_date, year, quarter, month, day, day_name, month_name, is_weekend)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        batch_data = []
        for i in range(delta.days + 1):
            curr = start_date + timedelta(days=i)
            date_key = int(curr.strftime("%Y%m%d"))
            is_weekend = curr.weekday() >= 5  # 5=Sat, 6=Sun

            record = (
                date_key,
                curr,
                curr.year,
                (curr.month - 1) // 3 + 1,
                curr.month,
                curr.day,
                curr.strftime("%A"),
                curr.strftime("%B"),
                is_weekend,
            )
            batch_data.append(record)

        if batch_data:
            cur.executemany(insert_query, batch_data)
            logging.info(f"✅ DIM_DATE refreshed. Inserted {len(batch_data)} rows.")

        conn.commit()

    except Exception as e:
        conn.rollback()
        logging.error(f"❌ DIM_DATE failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
