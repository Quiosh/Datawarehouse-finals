import psycopg2


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
        print("Starting DIM_PRODUCT Transformation...")

        # ==============================================================================
        # STEP 0: ENSURE DIMENSION + COLUMNS EXIST
        # ==============================================================================
        print("0. Ensuring dim_product table and staging columns exist...")

        # Create DIM_PRODUCT table
        # Note: 'subcategory' is excluded as requested.
        # We use 'category' to store the 'product_type' from staging.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_product (
                product_key   SERIAL PRIMARY KEY,
                product_name  VARCHAR(255),
                category      VARCHAR(100),
                price         DECIMAL(10,2),
                UNIQUE(product_name, category, price) -- distinct product versions
            );
        """)

        # Add product_key column to stg_product_data
        # (Assuming your staging table is named 'stg_product_data' based on previous patterns)
        cur.execute("""
            ALTER TABLE stg_product_list
            ADD COLUMN IF NOT EXISTS product_key INTEGER;
        """)

        # ==============================================================================
        # STEP 1: POPULATE DIMENSION
        # ==============================================================================
        print("1. Populating dim_product from stg_product_list...")

        # We select DISTINCT combinations. 
        # Note: We use lowercase column names (product_name, etc.) to match Postgres default storage.
        cur.execute("""
            INSERT INTO dim_product (product_name, category, price)
            SELECT DISTINCT 
                Product_name, 
                Product_type, -- Maps to 'category'
                Price
            FROM stg_product_list
            ON CONFLICT (product_name, category, price) DO NOTHING;
        """)

        # ==============================================================================
        # STEP 2: PROPAGATE SURROGATE KEY TO STAGING
        # ==============================================================================
        print("2. Updating stg_product_list with generated product_key...")

        # Link back to staging to fill the key
        cur.execute("""
            UPDATE stg_product_list s
            SET product_key = d.product_key
            FROM dim_product d
            WHERE s.Product_name = d.product_name
              AND s.Product_type = d.category
              AND s.Price = d.price;
        """)

        # ==============================================================================
        # STEP 3: COMMIT
        # ==============================================================================
        conn.commit()
        print("DIM_PRODUCT loaded and Staging keys updated successfully!")

        return {
            "status": "success",
            "message": "DIM_PRODUCT populated and keys propagated."
        }

    except Exception as e:
        conn.rollback()
        print(f"Error during transformation: {e}")
        raise e

    finally:
        cur.close()
        conn.close()
