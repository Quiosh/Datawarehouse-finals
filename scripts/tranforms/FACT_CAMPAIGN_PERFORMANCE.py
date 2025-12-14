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
        logging.info("Starting FACT_CAMPAIGN_PERFORMANCE processing...")

        # 2) Create Table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_campaign_performance (
                campaign_perf_key BIGSERIAL PRIMARY KEY,
                campaign_key BIGINT,
                date_key INT,
                total_orders INT,
                total_revenue NUMERIC,
                average_order_value NUMERIC,
                unique_customers INT,
                FOREIGN KEY (campaign_key) REFERENCES dim_campaign(campaign_key),
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
            );
        """)

        # 3) Clear Table
        cur.execute("TRUNCATE TABLE fact_campaign_performance CASCADE;")

        # 4) Load Aggregates
        logging.info("Aggregating campaign metrics...")
        cur.execute("""
            INSERT INTO fact_campaign_performance (
                campaign_key, date_key, total_orders, total_revenue, average_order_value, unique_customers
            )
            SELECT 
                campaign_key,
                date_key,
                COUNT(order_key) as total_orders,
                SUM(total_amount) as total_revenue,
                AVG(total_amount) as average_order_value,
                COUNT(DISTINCT user_key) as unique_customers
            FROM fact_orders
            WHERE campaign_key IS NOT NULL
            GROUP BY campaign_key, date_key;
        """)

        count = cur.rowcount
        conn.commit()
        logging.info(
            f" FACT_CAMPAIGN_PERFORMANCE loaded with {count} aggregated rows."
        )

    except Exception as e:
        conn.rollback()
        logging.error(f" FACT_CAMPAIGN_PERFORMANCE failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
