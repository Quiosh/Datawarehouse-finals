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
        print("Starting DIM_MERCHANT Transformation (String Keys)...")

        # ==============================================================================
        # STEP 0: REBUILD TABLE WITH VARCHAR KEY
        # ==============================================================================
        print("0. Recreating dim_merchant with VARCHAR key...")

        # Drop table to reset schema from Integer to String
        cur.execute("DROP TABLE IF EXISTS dim_merchant CASCADE;")

        cur.execute("""
            CREATE TABLE dim_merchant (
                merchant_key    VARCHAR(50) PRIMARY KEY, -- Changed to String/VARCHAR
                merchant_name   VARCHAR(255),
                city            VARCHAR(100),
                state          VARCHAR(100),
                UNIQUE(merchant_key)
            );
        """)

        # Reset the key column in staging to ensure it matches VARCHAR type
        cur.execute("""
            ALTER TABLE stg_merchant_data
            DROP COLUMN IF EXISTS merchant_key;
            
            ALTER TABLE stg_merchant_data
            ADD COLUMN merchant_key VARCHAR(50);
        """)

        # ==============================================================================
        # STEP 1: POPULATE DIRECTLY
        # ==============================================================================
        print("1. Populating dim_merchant using Merchant_id directly...")

        # Map Merchant_id directly to merchant_key
        # We assume columns are "Merchant_id", "Name", "City", "State" based on your image
        cur.execute("""
            INSERT INTO dim_merchant (merchant_key, merchant_name, city, state)
            SELECT DISTINCT 
                Merchant_id, 
                Name, 
                COALESCE(City, 'Unknown'),
                COALESCE(State, 'Unknown')
            FROM stg_merchant_data
            ON CONFLICT (merchant_key) DO NOTHING;
        """)

        # ==============================================================================
        # STEP 2: UPDATE STAGING
        # ==============================================================================
        print("2. Syncing merchant_key back to stg_merchant_data...")

        # Simple update: merchant_key is just the Merchant_id
        cur.execute("""
            UPDATE stg_merchant_data
            SET merchant_key = Merchant_id;
        """)

        # ==============================================================================
        # STEP 3: COMMIT
        # ==============================================================================
        conn.commit()
        print("DIM_MERCHANT loaded. Keys are now strings (e.g., 'MERCH001').")

        return {"status": "success"}

    except Exception as e:
        conn.rollback()
        print(f"Error during transformation: {e}")
        raise e

    finally:
        cur.close()
        conn.close()
