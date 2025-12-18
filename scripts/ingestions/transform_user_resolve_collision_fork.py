import psycopg2


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
    for table in [
        "stg_user_data",
        "stg_user_job",
        "stg_user_credit_card",
        "stg_order_data",
    ]:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS user_key BIGINT;")

    # Add staff_key/merchant_key to their tables
    cur.execute("ALTER TABLE stg_staff_data ADD COLUMN IF NOT EXISTS staff_key BIGINT;")
    cur.execute(
        "ALTER TABLE stg_merchant_data ADD COLUMN IF NOT EXISTS merchant_key BIGINT;"
    )

    # Add keys to the bridge table
    cur.execute(
        "ALTER TABLE stg_order_with_merchant_data ADD COLUMN IF NOT EXISTS staff_key BIGINT;"
    )
    cur.execute(
        "ALTER TABLE stg_order_with_merchant_data ADD COLUMN IF NOT EXISTS merchant_key BIGINT;"
    )


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
    dim_pk = f"{dim_name.split('_')[1]}_key"  # e.g. user_key

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
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    try:
        print("🚀 Starting Dimensions Creation (SCD2)...")

        # 1. Setup
        create_dimension_tables(cur)
        add_surrogate_columns(cur)

        # 2. Process Dimensions
        #    Args: (cur, dim_name, stg_name, id_col, name_col)
        process_scd2_dimension(cur, "dim_user", "stg_user_data", "user_id", "name")
        update_user_children(cur)  # Run immediately while temp map exists

        process_scd2_dimension(cur, "dim_staff", "stg_staff_data", "staff_id", "name")
        process_scd2_dimension(
            cur, "dim_merchant", "stg_merchant_data", "merchant_id", "name"
        )

        # 3. Update Shared Children
        update_staff_merchant_children(cur)

        # 4. Cleanup
        cur.execute("DROP TABLE IF EXISTS temp_map_dim_user;")
        cur.execute("DROP TABLE IF EXISTS temp_map_dim_staff;")
        cur.execute("DROP TABLE IF EXISTS temp_map_dim_merchant;")

        conn.commit()
        print("\n Dimensions Created and Keys Propagated Successfully!")
        return {"status": "success"}

    except Exception as e:
        conn.rollback()
        print(f"\n Error during transformation: {e}")
        raise e
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()
