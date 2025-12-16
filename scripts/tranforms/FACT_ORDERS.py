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
        logging.info("Starting FACT_ORDERS processing...")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_orders (
                order_key BIGSERIAL PRIMARY KEY,
                order_id TEXT UNIQUE NOT NULL,
                user_key BIGINT,
                merchant_key BIGINT,
                staff_key BIGINT,
                campaign_key BIGINT,
                date_key INT,
                delay_in_days INT,
                total_amount NUMERIC,
                FOREIGN KEY (user_key) REFERENCES dim_user(user_key),
                FOREIGN KEY (merchant_key) REFERENCES dim_merchant(merchant_key),
                FOREIGN KEY (staff_key) REFERENCES dim_staff(staff_key),
                FOREIGN KEY (campaign_key) REFERENCES dim_campaign(campaign_key),
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
            );
        """)

        cur.execute("TRUNCATE TABLE fact_orders CASCADE;")

        logging.info("Inserting data into fact_orders...")

        cur.execute("""
            INSERT INTO fact_orders (
                order_id, user_key, merchant_key, staff_key, campaign_key, date_key, delay_in_days, total_amount
            )
            SELECT 
                o.order_id,
                u.user_key,
                m.merchant_key,
                s.staff_key,
                c.campaign_key,
                CAST(TO_CHAR(o.transaction_date, 'YYYYMMDD') AS INTEGER) as date_key,
                d.delay_in_days,
                0 -- Placeholder, will be updated by order_items script
            FROM stg_order_data o
            LEFT JOIN stg_order_with_merchant_data om ON o.order_id = om.order_id
            LEFT JOIN stg_transactional_campaign_data tc ON o.order_id = tc.order_id
            LEFT JOIN stg_order_delays d ON o.order_id = d.order_id
            
            LEFT JOIN dim_user u ON o.user_id = u.source_user_id
            LEFT JOIN dim_merchant m ON om.merchant_id = m.source_merchant_id
            LEFT JOIN dim_staff s ON om.staff_id = s.source_staff_id
            LEFT JOIN dim_campaign c ON tc.campaign_id = c.campaign_id;
        """)

        count = cur.rowcount
        conn.commit()
        logging.info(f" FACT_ORDERS loaded with {count} rows.")

    except Exception as e:
        conn.rollback()
        logging.error(f" FACT_ORDERS failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
