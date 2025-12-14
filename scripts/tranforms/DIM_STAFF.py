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
        print("Starting DIM_STAFF Transformation...")

        # ==============================================================================
        # STEP 0: ENSURE DIMENSION + COLUMNS EXIST
        # - dim_staff: holds surrogate key (staff_key)
        # - Add staff_key columns to staging table (for easier Fact loading later)
        # ==============================================================================

        print("0. Ensuring dim_staff table and staging columns exist...")

        # Create DIM_STAFF table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_staff (
                staff_key   SERIAL PRIMARY KEY,
                staff_name  VARCHAR(255),
                region      VARCHAR(100),
                UNIQUE(staff_name, region) -- Prevent exact duplicates
            );
        """)

        # Add staff_key column to stg_staff_data (Idempotent)
        cur.execute("""
            ALTER TABLE "stg_staff_data"
            ADD COLUMN IF NOT EXISTS staff_key INTEGER;
        """)

        # ==============================================================================
        # STEP 1: POPULATE DIMENSION
        # Select distinct staff from staging and insert into dimension
        # ==============================================================================

        print("1. Populating dim_staff from stg_staff_data...")

        # We use ON CONFLICT DO NOTHING to avoid crashing if run multiple times
        cur.execute("""
            INSERT INTO dim_staff (staff_name, region)
            SELECT DISTINCT 
                Name, 
                COALESCE(State, 'Unknown')
            FROM "stg_staff_data"
            ON CONFLICT (staff_name, region) DO NOTHING;
        """)

        # ==============================================================================
        # STEP 2: PROPAGATE SURROGATE KEY TO STAGING
        # Update stg_staff_data with the new staff_key from dim_staff
        # ==============================================================================

        print("2. Updating stg_staff_data with generated staff_key...")

        # We join back to the dimension to retrieve the generated ID
        cur.execute("""
            UPDATE "stg_staff_data" s
            SET staff_key = d.staff_key
            FROM dim_staff d
            WHERE s.Name = d.staff_name
              AND COALESCE(s.State, 'Unknown') = d.region;
        """)

        # ==============================================================================
        # STEP 3: COMMIT
        # ==============================================================================

        conn.commit()
        print("DIM_STAFF loaded and Staging keys updated successfully!")

        return {
            "status": "success",
            "message": "DIM_STAFF populated and keys propagated.",
        }

    except Exception as e:
        conn.rollback()
        print(f"Error during transformation: {e}")
        raise e

    finally:
        cur.close()
        conn.close()
