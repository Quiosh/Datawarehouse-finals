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
        print("Starting User Surrogate Key + History Resolution...")

        # ==============================================================================
        # STEP 0: ENSURE DIMENSION + COLUMNS EXIST
        # ==============================================================================

        print("0. Resetting dim_user schema...")

        # ⚠️ CRITICAL FIX: Drop the old table to fix the "UndefinedColumn" error
        cur.execute("DROP TABLE IF EXISTS dim_user CASCADE;")

        # Now create it fresh with the CORRECT columns
        cur.execute("""
            CREATE TABLE dim_user (
                user_key        BIGSERIAL PRIMARY KEY,
                source_user_id  TEXT NOT NULL,
                name            TEXT,
                creation_date   TIMESTAMP,
                valid_from      TIMESTAMP,
                valid_to        TIMESTAMP,
                is_current      BOOLEAN DEFAULT TRUE,
                birthdate       DATE,
                gender          TEXT,
                user_type       TEXT,
                city            TEXT,
                state           TEXT,
                country         TEXT,
                job_title       TEXT,
                job_level       TEXT
            );
        """)

        # Create Index
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dim_user_source_date
            ON dim_user (source_user_id, valid_from);
        """)

        # Add user_key columns to staging tables
        cur.execute("ALTER TABLE stg_user_data ADD COLUMN IF NOT EXISTS user_key BIGINT;")
        cur.execute("ALTER TABLE stg_user_job ADD COLUMN IF NOT EXISTS user_key BIGINT;")
        cur.execute("ALTER TABLE stg_user_credit_card ADD COLUMN IF NOT EXISTS user_key BIGINT;")
        cur.execute("ALTER TABLE stg_order_data ADD COLUMN IF NOT EXISTS user_key BIGINT;")

        # ==============================================================================
        # STEP 1: BUILD USER VERSIONS (VALID_FROM / VALID_TO)
        # ==============================================================================

        print("1. Building user version windows from stg_user_data...")

        cur.execute("""
            DROP TABLE IF EXISTS temp_user_versions;

            CREATE TEMP TABLE temp_user_versions AS
            SELECT
                user_id AS source_user_id,
                name,
                creation_date,
                creation_date AS valid_from,
                LEAD(creation_date) OVER (
                    PARTITION BY user_id 
                    ORDER BY creation_date ASC
                ) AS valid_to,
                street, city, state, country, birthdate, gender, user_type
            FROM stg_user_data;
        """)

        # ==============================================================================
        # STEP 2: INSERT NEW USERS
        # ==============================================================================

        print("2. Inserting new user versions into dim_user...")

        cur.execute("""
            INSERT INTO dim_user (
                source_user_id, name, creation_date, valid_from, valid_to, is_current,
                city, state, country, birthdate, gender, user_type
            )
            SELECT
                source_user_id,
                name,
                creation_date,
                valid_from,
                valid_to,
                CASE WHEN valid_to IS NULL THEN TRUE ELSE FALSE END,
                city, state, country, birthdate, gender, user_type
            FROM temp_user_versions;
        """)

        # ==============================================================================
        # STEP 3: UPDATE JOB DETAILS
        # ==============================================================================
        print("3. Updating Job Details...")
        cur.execute("""
            UPDATE dim_user d
            SET job_title = j.job_title,
                job_level = j.job_level
            FROM stg_user_job j
            WHERE d.source_user_id = j.user_id;
        """)

        # ==============================================================================
        # STEP 4: BUILD MAPPING & UPDATE STAGING
        # ==============================================================================

        print("4. Mapping Surrogate Keys back to Staging...")

        cur.execute("""
            DROP TABLE IF EXISTS temp_user_remapping;
            CREATE TEMP TABLE temp_user_remapping AS
            SELECT u.user_id, u.creation_date, d.user_key
            FROM stg_user_data u
            JOIN dim_user d ON u.user_id = d.source_user_id AND u.creation_date = d.creation_date;
        """)

        cur.execute("""
            UPDATE stg_user_data u
            SET user_key = m.user_key, possible_duplicate = FALSE
            FROM temp_user_remapping m
            WHERE u.user_id = m.user_id AND u.creation_date = m.creation_date;
        """)

        cur.execute("""
            UPDATE stg_order_data o
            SET user_key = d.user_key
            FROM dim_user d
            WHERE o.user_id = d.source_user_id
              AND o.transaction_date >= d.valid_from
              AND (o.transaction_date < d.valid_to OR d.valid_to IS NULL);
        """)

        # Cleanup
        cur.execute("DROP TABLE IF EXISTS temp_user_versions;")
        cur.execute("DROP TABLE IF EXISTS temp_user_remapping;")
        
        conn.commit()
        print("✅ Success: dim_user rebuilt and User Keys assigned.")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error during transformation: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()