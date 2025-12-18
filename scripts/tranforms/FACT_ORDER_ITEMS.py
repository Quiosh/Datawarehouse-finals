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
        logging.info("Starting FACT_ORDER_ITEMS processing...")

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fact_order_items (
                order_item_key BIGSERIAL PRIMARY KEY,
                order_id TEXT,
                product_key BIGINT,
                user_key BIGINT,
                merchant_key BIGINT,
                campaign_key BIGINT,
                date_key INT,
                quantity INT,
                unit_price NUMERIC,
                total_price NUMERIC,
                FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
                FOREIGN KEY (user_key) REFERENCES dim_user(user_key),
                FOREIGN KEY (merchant_key) REFERENCES dim_merchant(merchant_key),
                FOREIGN KEY (campaign_key) REFERENCES dim_campaign(campaign_key),
                FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
            );
        """)

        cur.execute("TRUNCATE TABLE fact_order_items CASCADE;")

        logging.info("Inserting data into fact_order_items...")
        cur.execute("""
            INSERT INTO fact_order_items (
                order_id, product_key, user_key, merchant_key, campaign_key, date_key, quantity, unit_price, total_price
            )
            WITH ordered_products AS (
                SELECT 
                    order_id, 
                    product_id,
                    ROW_NUMBER() OVER (PARTITION BY order_id) as rn
                FROM stg_line_item_data_products
            ),
            ordered_prices AS (
                SELECT 
                    order_id,
                    price,
                    quantity,
                    ROW_NUMBER() OVER (PARTITION BY order_id) as rn
                FROM stg_line_item_data_prices
            )
            SELECT 
                op.order_id,
                dp.product_key,
                fo.user_key,
                fo.merchant_key,
                fo.campaign_key,
                fo.date_key,
                opr.quantity,
                opr.price as unit_price,
                (opr.quantity * opr.price) as total_price
            FROM ordered_products op
            JOIN ordered_prices opr ON op.order_id = opr.order_id AND op.rn = opr.rn
            JOIN dim_product dp ON op.product_id = dp.product_id
            JOIN fact_orders fo ON op.order_id = fo.order_id;
        """)

        logging.info(f"Inserted {cur.rowcount} line items.")

        logging.info("Updating total_amount in FACT_ORDERS...")
        cur.execute("""
            UPDATE fact_orders fo
            SET total_amount = sub.order_total
            FROM (
                SELECT order_id, SUM(total_price) as order_total
                FROM fact_order_items
                GROUP BY order_id
            ) sub
            WHERE fo.order_id = sub.order_id;
        """)

        conn.commit()
        logging.info(" FACT_ORDER_ITEMS loaded and Header Totals updated.")

    except Exception as e:
        conn.rollback()
        logging.error(f" FACT_ORDER_ITEMS failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()