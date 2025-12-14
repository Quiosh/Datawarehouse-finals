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
        print("Starting DIM_CAMPAIGN Transformation...")

        # ==============================================================================
        # STEP 0: ENSURE DIMENSION + COLUMNS EXIST
        # ==============================================================================
        print("0. Ensuring dim_campaign table and staging columns exist...")

        # Create DIM_CAMPAIGN table
        # We assume 'discount_value' maps to the 'Discount' column in staging
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_campaign (
                campaign_key    SERIAL PRIMARY KEY,
                campaign_name   VARCHAR(255),
                discount_value  DECIMAL(10,2),
                UNIQUE(campaign_name, discount_value)
            );
        """)

        # Add campaign_key column to stg_campaign_data
        # (Assuming your staging table is named 'stg_campaign_data')
        cur.execute("""
            ALTER TABLE stg_campaign_data
            ADD COLUMN IF NOT EXISTS campaign_key INTEGER;
        """)

        # ==============================================================================
        # STEP 1: POPULATE DIMENSION
        # ==============================================================================
        print("1. Populating dim_campaign from stg_campaign_data...")

        # We map:
        # stg.campaign_name -> dim.campaign_name
        # stg.discount      -> dim.discount_value
        # using COALESCE(..., 0) to handle potential NULLs in discount
        cur.execute("""
            INSERT INTO dim_campaign (campaign_name, discount_value)
            SELECT DISTINCT 
                campaign_name, 
                COALESCE(discount, 0)
            FROM stg_campaign_data
            ON CONFLICT (campaign_name, discount_value) DO NOTHING;
        """)

        # ==============================================================================
        # STEP 2: PROPAGATE SURROGATE KEY TO STAGING
        # ==============================================================================
        print("2. Updating stg_campaign_data with generated campaign_key...")

        # Update the staging table so we can link it to facts later
        cur.execute("""
            UPDATE stg_campaign_data s
            SET campaign_key = d.campaign_key
            FROM dim_campaign d
            WHERE s.campaign_name = d.campaign_name
              AND COALESCE(s.discount, 0) = d.discount_value;
        """)

        # ==============================================================================
        # STEP 3: COMMIT
        # ==============================================================================
        conn.commit()
        print("DIM_CAMPAIGN loaded and Staging keys updated successfully!")

        return {
            "status": "success",
            "message": "DIM_CAMPAIGN populated and keys propagated.",
        }

    except Exception as e:
        conn.rollback()
        print(f"Error during transformation: {e}")
        raise e

    finally:
        cur.close()
        conn.close()
