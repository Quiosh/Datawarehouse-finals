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
        # - dim_user: holds surrogate key (user_key) + history
        # - Add user_key columns to staging tables (if not yet present)
        # ==============================================================================

        print("0. Ensuring dim_user and user_key columns exist...")

        # Create dimension table if not exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dim_user (
                user_key        BIGSERIAL PRIMARY KEY,
                source_user_id  TEXT NOT NULL,
                name            TEXT,
                creation_date   DATE,
                valid_from      DATE NOT NULL,
                valid_to        DATE,
                is_current      BOOLEAN DEFAULT TRUE
            );
        """)

        # Helpful index for joining by business key + date
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dim_user_source_date
            ON dim_user (source_user_id, valid_from);
        """)

        # Add user_key columns to staging tables (idempotent)
        cur.execute("""
            ALTER TABLE stg_user_data
            ADD COLUMN IF NOT EXISTS user_key BIGINT;
        """)
        cur.execute("""
            ALTER TABLE stg_user_job
            ADD COLUMN IF NOT EXISTS user_key BIGINT;
        """)
        cur.execute("""
            ALTER TABLE stg_user_credit_card
            ADD COLUMN IF NOT EXISTS user_key BIGINT;
        """)
        cur.execute("""
            ALTER TABLE stg_order_data
            ADD COLUMN IF NOT EXISTS user_key BIGINT;
        """)

        # ==============================================================================
        # STEP 1: BUILD USER VERSIONS (VALID_FROM / VALID_TO)
        # From stg_user_data, define history windows for each source_user_id
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
                ) AS valid_to
            FROM stg_user_data;
        """)

        # ==============================================================================
        # STEP 2: INSERT/UPDATE dim_user
        # Insert new versions into dim_user (do not duplicate if re-run)
        # ==============================================================================

        print("2. Inserting new user versions into dim_user...")

        # Insert only rows that are not already in dim_user
        # We match by (source_user_id, creation_date) as a unique version
        cur.execute("""
            INSERT INTO dim_user (
                source_user_id,
                name,
                creation_date,
                valid_from,
                valid_to,
                is_current
            )
            SELECT
                v.source_user_id,
                v.name,
                v.creation_date,
                v.valid_from,
                v.valid_to,
                CASE WHEN v.valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
            FROM temp_user_versions v
            LEFT JOIN dim_user d
              ON d.source_user_id = v.source_user_id
             AND d.creation_date = v.creation_date
            WHERE d.user_key IS NULL;
        """)

        # Optional: refresh is_current flags if needed
        cur.execute("""
            UPDATE dim_user d
            SET is_current = (d.valid_to IS NULL);
        """)

        # ==============================================================================
        # STEP 3: BUILD MAPPING (BUSINESS KEY -> SURROGATE KEY)
        # temp_user_remapping: from staging rows to dim_user.user_key
        # ==============================================================================

        print("3. Building mapping from stg_user_data to dim_user.user_key...")

        cur.execute("""
            DROP TABLE IF EXISTS temp_user_remapping;

            CREATE TEMP TABLE temp_user_remapping AS
            SELECT
                u.user_id AS original_user_id,
                u.name,
                u.creation_date,
                d.user_key,
                d.valid_from,
                d.valid_to
            FROM stg_user_data u
            JOIN dim_user d
              ON d.source_user_id = u.user_id
             AND d.creation_date = u.creation_date;
            
            CREATE INDEX idx_remap_orig ON temp_user_remapping(original_user_id);
            CREATE INDEX idx_remap_name ON temp_user_remapping(name);
        """)

        # ==============================================================================
        # STEP 4: UPDATE STAGING TABLES WITH SURROGATE KEY
        # We now fill user_key everywhere, leaving user_id as the business key
        # ==============================================================================

        print("4A. Updating stg_user_data (set user_key, clear duplicates)...")

        cur.execute("""
            UPDATE stg_user_data u
            SET user_key = m.user_key,
                possible_duplicate = FALSE   -- they are now uniquely identified
            FROM temp_user_remapping m
            WHERE u.user_id = m.original_user_id
              AND u.creation_date = m.creation_date;
        """)

        print("4B. Updating stg_user_job using (user_id + name) match...")

        cur.execute("""
            UPDATE stg_user_job j
            SET user_key = m.user_key
            FROM temp_user_remapping m
            WHERE j.user_id = m.original_user_id
              AND j.name = m.name;
        """)

        print("4C. Updating stg_user_credit_card using (user_id + name) match...")

        cur.execute("""
            UPDATE stg_user_credit_card c
            SET user_key = m.user_key
            FROM temp_user_remapping m
            WHERE c.user_id = m.original_user_id
              AND c.name = m.name;
        """)

        print("4D. Updating stg_order_data using date-window logic...")

        cur.execute("""
            UPDATE stg_order_data o
            SET user_key = m.user_key
            FROM temp_user_remapping m
            WHERE o.user_id = m.original_user_id
              AND o.transaction_date >= m.valid_from
              AND (o.transaction_date < m.valid_to OR m.valid_to IS NULL);
        """)

        # ==============================================================================
        # STEP 5: CLEANUP
        # ==============================================================================

        print("5. Cleaning up temp tables...")
        cur.execute("DROP TABLE IF EXISTS temp_user_versions;")
        cur.execute("DROP TABLE IF EXISTS temp_user_remapping;")
        
        conn.commit()
        print("✅ Surrogate key + history resolution complete!")

        return {
            "status": "success",
            "message": "Surrogate user_key generated in dim_user and propagated to staging tables."
        }

    except Exception as e:
        conn.rollback()
        print(f"❌ Error during transformation: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()