import psycopg2

<<<<<<< HEAD
def create_dimension_tables(cur):
    print("0. Creating Dimension Tables (User, Staff, Merchant)...")
    
    # --- 1. DIM_USER ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_user (
            user_key        BIGSERIAL PRIMARY KEY,
            source_user_id  TEXT NOT NULL,
            name            TEXT,
            valid_from      TIMESTAMP NOT NULL,
            valid_to        TIMESTAMP,
            is_current      BOOLEAN DEFAULT TRUE
        );
        CREATE INDEX IF NOT EXISTS idx_dim_user_lookup ON dim_user(source_user_id, valid_from);
    """)

    # --- 2. DIM_STAFF ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_staff (
            staff_key       BIGSERIAL PRIMARY KEY,
            source_staff_id TEXT NOT NULL,
            name            TEXT,
            valid_from      TIMESTAMP NOT NULL,
            valid_to        TIMESTAMP,
            is_current      BOOLEAN DEFAULT TRUE
        );
        CREATE INDEX IF NOT EXISTS idx_dim_staff_lookup ON dim_staff(source_staff_id, valid_from);
    """)

    # --- 3. DIM_MERCHANT ---
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_merchant (
            merchant_key       BIGSERIAL PRIMARY KEY,
            source_merchant_id TEXT NOT NULL,
            name               TEXT,
            valid_from         TIMESTAMP NOT NULL,
            valid_to           TIMESTAMP,
            is_current         BOOLEAN DEFAULT TRUE
        );
        CREATE INDEX IF NOT EXISTS idx_dim_merch_lookup ON dim_merchant(source_merchant_id, valid_from);
    """)

def add_surrogate_columns(cur):
    print("0B. Ensuring staging tables have '_key' columns...")
    
    # Add user_key to user-related tables
    for table in ['stg_user_data', 'stg_user_job', 'stg_user_credit_card', 'stg_order_data']:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS user_key BIGINT;")

    # Add staff_key/merchant_key to their tables
    cur.execute("ALTER TABLE stg_staff_data ADD COLUMN IF NOT EXISTS staff_key BIGINT;")
    cur.execute("ALTER TABLE stg_merchant_data ADD COLUMN IF NOT EXISTS merchant_key BIGINT;")
    
    # Add keys to the bridge table
    cur.execute("ALTER TABLE stg_order_with_merchant_data ADD COLUMN IF NOT EXISTS staff_key BIGINT;")
    cur.execute("ALTER TABLE stg_order_with_merchant_data ADD COLUMN IF NOT EXISTS merchant_key BIGINT;")

# ==============================================================================
# LOGIC BLOCK: GENERIC SCD2 LOADER
# ==============================================================================
def process_scd2_dimension(cur, dim_name, stg_name, id_col, name_col):
    """
    Generic function to load a staging table into a dimension using SCD Type 2 logic.
    """
    print(f"\n--- Processing {dim_name} from {stg_name} ---")
    
    # 1. Calculate Versions (Time Windows)
    print("   -> Calculating versions...")
    cur.execute(f"DROP TABLE IF EXISTS temp_versions_{dim_name};")
    cur.execute(f"""
        CREATE TEMP TABLE temp_versions_{dim_name} AS
        SELECT 
            {id_col} AS source_id,
            {name_col},
            creation_date AS valid_from,
            LEAD(creation_date) OVER (
                PARTITION BY {id_col} ORDER BY creation_date ASC
            ) AS valid_to
        FROM {stg_name};
    """)

    # 2. Insert New Versions into Dimension
    print("   -> Loading dimension...")
    dim_pk = f"{dim_name.split('_')[1]}_key" # e.g. user_key
    
    cur.execute(f"""
        INSERT INTO {dim_name} (source_{id_col}, {name_col}, valid_from, valid_to, is_current)
        SELECT 
            v.source_id, v.{name_col}, v.valid_from, v.valid_to,
            CASE WHEN v.valid_to IS NULL THEN TRUE ELSE FALSE END
        FROM temp_versions_{dim_name} v
        WHERE NOT EXISTS (
            SELECT 1 FROM {dim_name} d 
            WHERE d.source_{id_col} = v.source_id 
              AND d.{name_col} = v.{name_col}
              AND d.valid_from = v.valid_from
        );
    """)

    # 3. Create Mapping Table (Source -> Key)
    print("   -> Creating mapping table...")
    cur.execute(f"DROP TABLE IF EXISTS temp_map_{dim_name};")
    cur.execute(f"""
        CREATE TEMP TABLE temp_map_{dim_name} AS
        SELECT 
            s.{id_col} as original_id,
            s.{name_col},
            s.creation_date,
            d.{dim_pk},
            d.valid_from,
            d.valid_to
        FROM {stg_name} s
        JOIN {dim_name} d 
          ON d.source_{id_col} = s.{id_col} 
         AND d.{name_col} = s.{name_col}
         AND d.valid_from = s.creation_date;
    """)
    cur.execute(f"CREATE INDEX idx_map_{dim_name} ON temp_map_{dim_name}(original_id);")

    # 4. Update the Staging Table itself
    print(f"   -> Updating {stg_name} with keys...")
    cur.execute(f"""
        UPDATE {stg_name} s
        SET {dim_pk} = m.{dim_pk}, possible_duplicate = FALSE
        FROM temp_map_{dim_name} m
        WHERE s.{id_col} = m.original_id AND s.creation_date = m.creation_date;
    """)

# ==============================================================================
# LOGIC BLOCK: UPDATE CHILD TABLES
# ==============================================================================

def update_user_children(cur):
    print("\n--- Updating USER Children ---")
    # 1. User Jobs (Match by Name)
    print("   -> Updating stg_user_job...")
    cur.execute("""
        UPDATE stg_user_job c
        SET user_key = m.user_key, possible_duplicate = FALSE
        FROM temp_map_dim_user m
        WHERE c.user_id = m.original_id AND c.name = m.name;
    """)
    
    # 2. Credit Cards (Match by Name)
    print("   -> Updating stg_user_credit_card...")
    cur.execute("""
        UPDATE stg_user_credit_card c
        SET user_key = m.user_key
        FROM temp_map_dim_user m
        WHERE c.user_id = m.original_id AND c.name = m.name;
    """)

    # 3. Orders (Match by Date Window)
    print("   -> Updating stg_order_data...")
    cur.execute("""
        UPDATE stg_order_data o
        SET user_key = m.user_key
        FROM temp_map_dim_user m
        WHERE o.user_id = m.original_id
          AND o.transaction_date >= m.valid_from
          AND (o.transaction_date < m.valid_to OR m.valid_to IS NULL);
    """)

def update_staff_merchant_children(cur):
    print("\n--- Updating STAFF/MERCHANT Children ---")
    
    # The child table 'stg_order_with_merchant_data' needs DATES from 'stg_order_data'
    # to perform the lookup.
    
    print("   -> Updating stg_order_with_merchant_data (Staff Keys)...")
    cur.execute("""
        UPDATE stg_order_with_merchant_data c
        SET staff_key = m.staff_key
        FROM temp_map_dim_staff m, stg_order_data o
        WHERE c.staff_id = m.original_id
          AND c.order_id = o.order_id
          AND o.transaction_date >= m.valid_from
          AND (o.transaction_date < m.valid_to OR m.valid_to IS NULL);
    """)

    print("   -> Updating stg_order_with_merchant_data (Merchant Keys)...")
    cur.execute("""
        UPDATE stg_order_with_merchant_data c
        SET merchant_key = m.merchant_key
        FROM temp_map_dim_merchant m, stg_order_data o
        WHERE c.merchant_id = m.original_id
          AND c.order_id = o.order_id
          AND o.transaction_date >= m.valid_from
          AND (o.transaction_date < m.valid_to OR m.valid_to IS NULL);
    """)

def main():
=======
def main():
    # 1) Connect to Postgres
>>>>>>> 5eea1712add391a654aa9487ad3c54f91de16d19
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    try:
<<<<<<< HEAD
        print("🚀 Starting Dimensions Creation (SCD2)...")
        
        # 1. Setup
        create_dimension_tables(cur)
        add_surrogate_columns(cur)

        # 2. Process Dimensions
        #    Args: (cur, dim_name, stg_name, id_col, name_col)
        process_scd2_dimension(cur, "dim_user", "stg_user_data", "user_id", "name")
        update_user_children(cur) # Run immediately while temp map exists

        process_scd2_dimension(cur, "dim_staff", "stg_staff_data", "staff_id", "name")
        process_scd2_dimension(cur, "dim_merchant", "stg_merchant_data", "merchant_id", "name")
        
        # 3. Update Shared Children
        update_staff_merchant_children(cur)

        # 4. Cleanup
        cur.execute("DROP TABLE IF EXISTS temp_map_dim_user;")
        cur.execute("DROP TABLE IF EXISTS temp_map_dim_staff;")
        cur.execute("DROP TABLE IF EXISTS temp_map_dim_merchant;")
        
        conn.commit()
        print("\n✅ Dimensions Created and Keys Propagated Successfully!")
        return {"status": "success"}
=======
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
>>>>>>> 5eea1712add391a654aa9487ad3c54f91de16d19

    except Exception as e:
        conn.rollback()
        print(f"❌ Error during transformation: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

if __name__ == '__main__':
    main()