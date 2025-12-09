import psycopg2

def resolve_users(cur):
    print("\n--- Resolving USER Collisions ---")
    
    # 1. Create Mapping (Old ID -> New Unique ID)
    cur.execute("DROP TABLE IF EXISTS temp_user_remapping;")
    cur.execute("""
        CREATE TABLE temp_user_remapping AS
        SELECT 
            user_id as original_id,
            name,
            creation_date,
            CASE 
                WHEN possible_duplicate = TRUE 
                THEN user_id || '_HIST_' || to_char(creation_date, 'YYYYMMDD') 
                ELSE user_id 
            END as new_id,
            creation_date as valid_from,
            LEAD(creation_date) OVER (PARTITION BY user_id ORDER BY creation_date ASC) as valid_to
        FROM stg_user_data;
    """)
    cur.execute("CREATE INDEX idx_user_map_orig ON temp_user_remapping(original_id);")

    # 2. Update Main Table (stg_user_data)
    #    - Update ID to new unique ID
    #    - Set possible_duplicate = FALSE (Because it's now unique!)
    print("Updating stg_user_data...")
    cur.execute("""
        UPDATE stg_user_data main
        SET user_id = m.new_id, possible_duplicate = FALSE
        FROM temp_user_remapping m
        WHERE main.user_id = m.original_id 
          AND main.creation_date = m.creation_date
          AND main.possible_duplicate = TRUE;
    """)

    # 3. Update Child Table: stg_user_job
    #    - We match by NAME to find the right user version.
    #    - We ALSO clear the 'possible_duplicate' flag here since we are fixing it.
    print("Updating stg_user_job (Name Match)...")
    cur.execute("""
        UPDATE stg_user_job child
        SET user_id = m.new_id, possible_duplicate = FALSE
        FROM temp_user_remapping m
        WHERE child.user_id = m.original_id 
          AND child.name = m.name
          AND m.new_id != m.original_id;
    """)

    # 4. Update Other Child Tables (Standard)
    print("Updating stg_user_credit_card (Name Match)...")
    cur.execute("""
        UPDATE stg_user_credit_card child
        SET user_id = m.new_id
        FROM temp_user_remapping m
        WHERE child.user_id = m.original_id 
          AND child.name = m.name
          AND m.new_id != m.original_id;
    """)

    print("Updating stg_order_data (Date Match)...")
    cur.execute("""
        UPDATE stg_order_data child
        SET user_id = m.new_id
        FROM temp_user_remapping m
        WHERE child.user_id = m.original_id
          AND child.transaction_date >= m.valid_from
          AND (child.transaction_date < m.valid_to OR m.valid_to IS NULL)
          AND m.new_id != m.original_id;
    """)
    
    cur.execute("DROP TABLE temp_user_remapping;")


def resolve_staff(cur):
    print("\n--- Resolving STAFF Collisions ---")

    # 1. Create Mapping
    cur.execute("DROP TABLE IF EXISTS temp_staff_remapping;")
    cur.execute("""
        CREATE TABLE temp_staff_remapping AS
        SELECT 
            staff_id as original_id,
            creation_date,
            CASE 
                WHEN possible_duplicate = TRUE 
                THEN staff_id || '_HIST_' || to_char(creation_date, 'YYYYMMDD') 
                ELSE staff_id 
            END as new_id,
            creation_date as valid_from
        FROM stg_staff_data;
    """)
    cur.execute("CREATE INDEX idx_staff_map_orig ON temp_staff_remapping(original_id);")

    # 2. Update Main Table (stg_staff_data)
    print("Updating stg_staff_data...")
    cur.execute("""
        UPDATE stg_staff_data main
        SET staff_id = m.new_id, possible_duplicate = FALSE
        FROM temp_staff_remapping m
        WHERE main.staff_id = m.original_id 
          AND main.creation_date = m.creation_date
          AND main.possible_duplicate = TRUE;
    """)

    # 3. Update Child Table (stg_order_with_merchant_data)
    print("Updating stg_order_with_merchant_data (via Order Date)...")
    cur.execute("""
        UPDATE stg_order_with_merchant_data child
        SET staff_id = m.new_id
        FROM temp_staff_remapping m, stg_order_data orders
        WHERE child.staff_id = m.original_id
          AND child.order_id = orders.order_id
          AND orders.transaction_date >= m.valid_from
          AND m.new_id != m.original_id;
    """)

    cur.execute("DROP TABLE temp_staff_remapping;")


def resolve_merchants(cur):
    print("\n--- Resolving MERCHANT Collisions ---")

    # 1. Create Mapping
    cur.execute("DROP TABLE IF EXISTS temp_merchant_remapping;")
    cur.execute("""
        CREATE TABLE temp_merchant_remapping AS
        SELECT 
            merchant_id as original_id,
            creation_date,
            CASE 
                WHEN possible_duplicate = TRUE 
                THEN merchant_id || '_HIST_' || to_char(creation_date, 'YYYYMMDD') 
                ELSE merchant_id 
            END as new_id,
            creation_date as valid_from
        FROM stg_merchant_data;
    """)
    cur.execute("CREATE INDEX idx_merch_map_orig ON temp_merchant_remapping(original_id);")

    # 2. Update Main Table (stg_merchant_data)
    print("Updating stg_merchant_data...")
    cur.execute("""
        UPDATE stg_merchant_data main
        SET merchant_id = m.new_id, possible_duplicate = FALSE
        FROM temp_merchant_remapping m
        WHERE main.merchant_id = m.original_id 
          AND main.creation_date = m.creation_date
          AND main.possible_duplicate = TRUE;
    """)

    # 3. Update Child Table (stg_order_with_merchant_data)
    print("Updating stg_order_with_merchant_data (via Order Date)...")
    cur.execute("""
        UPDATE stg_order_with_merchant_data child
        SET merchant_id = m.new_id
        FROM temp_merchant_remapping m, stg_order_data orders
        WHERE child.merchant_id = m.original_id
          AND child.order_id = orders.order_id
          AND orders.transaction_date >= m.valid_from
          AND m.new_id != m.original_id;
    """)

    cur.execute("DROP TABLE temp_merchant_remapping;")


def main():
    # Connect to Postgres
    conn = psycopg2.connect(
        host="db",
        port=5432,
        user="postgres",
        password="shopzada",
        dbname="shopzada",
    )
    cur = conn.cursor()

    try:
        print("🚀 Starting Global Collision Resolution...")
        
        # We run Users first, then Staff/Merchants
        resolve_users(cur)
        resolve_staff(cur)
        resolve_merchants(cur)

        conn.commit()
        print("\n✅ All ID Collisions Resolved Successfully!")
        return {"status": "success"}

    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error during transformation: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()