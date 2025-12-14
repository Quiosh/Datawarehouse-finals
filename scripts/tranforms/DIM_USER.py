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
        print("Starting DIM_USER Transformation (String Keys)...")

        # ==============================================================================
        # STEP 0: REBUILD TABLE WITH VARCHAR KEY
        # ==============================================================================
        print("0. Recreating dim_user with VARCHAR key...")

        # Drop table to reset schema from Integer to String
        cur.execute("DROP TABLE IF EXISTS dw_user CASCADE;")

        cur.execute("""
            CREATE TABLE dw_user (
                user_key    VARCHAR(50) PRIMARY KEY,
                user_name   VARCHAR(255),
                segment         VARCHAR(50),
                UNIQUE(user_key)
            );
        """)

        # Reset the key column in staging to ensure it matches VARCHAR type
        cur.execute("""
            ALTER TABLE stg_user_data
            DROP COLUMN IF EXISTS user_key;
            
            ALTER TABLE stg_user_data
            ADD COLUMN user_key VARCHAR(50);
        """)

        # ==============================================================================
        # STEP 1: POPULATE DIRECTLY
        # ==============================================================================
        print("1. Populating dim_user using User_id directly...")

        # Map User_id directly to user_key
        cur.execute("""
            INSERT INTO dw_user (user_key, user_name, segment)
            SELECT DISTINCT 
                User_id, 
                name, 
                COALESCE(user_type, 'Unknown')
            FROM stg_user_data
            ON CONFLICT (user_key) DO NOTHING;
        """)

        # ==============================================================================
        # STEP 2: UPDATE STAGING
        # ==============================================================================
        print("2. Syncing user_key back to stg_user_data...")

        cur.execute("""
            UPDATE stg_user_data
            SET user_key = User_id;
        """)

        # ==============================================================================
        # STEP 3: COMMIT
        # ==============================================================================
        conn.commit()
        print("DIM_USER loaded. Keys are now strings (e.g., 'USER00128').")

        return {"status": "success"}

    except Exception as e:
        conn.rollback()
        print(f"Error during transformation: {e}")
        raise e

    finally:
        cur.close()
        conn.close()
