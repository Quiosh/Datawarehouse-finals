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
        print("Starting User ID Collision Resolution...")

        # ==============================================================================
        # STEP 1: CREATE MAPPING TABLE
        # Create a "Rosetta Stone" that maps (Old ID + Time) -> New Unique ID
        # ==============================================================================
        print("1. Creating mapping table for duplicates...")
        cur.execute("""
            DROP TABLE IF EXISTS temp_user_remapping;

            CREATE TABLE temp_user_remapping AS
            SELECT 
                user_id as original_user_id,
                name,
                creation_date,
                -- Generate NEW ID for duplicates: "USER123_HIST_20200101"
                CASE 
                    WHEN possible_duplicate = TRUE 
                    THEN user_id || '_HIST_' || to_char(creation_date, 'YYYYMMDD') 
                    ELSE user_id 
                END as new_user_id,
                
                -- Define Validity Window (Start Date)
                creation_date as valid_from,
                
                -- Define Validity Window (End Date = Creation date of the NEXT version of this user)
                LEAD(creation_date) OVER (
                    PARTITION BY user_id ORDER BY creation_date ASC
                ) as valid_to
            FROM stg_user_data;

            -- Create indexes to speed up the updates
            CREATE INDEX idx_remap_orig ON temp_user_remapping(original_user_id);
            CREATE INDEX idx_remap_name ON temp_user_remapping(name);
        """)

        # ==============================================================================
        # STEP 2: FIX THE MAIN USER TABLE
        # Update the duplicates in stg_user_data to use their new unique IDs
        # ==============================================================================
        print("2. Updating stg_user_data with new unique IDs...")
        cur.execute("""
            UPDATE stg_user_data u
            SET user_id = m.new_user_id,
                possible_duplicate = FALSE -- They are now unique, so no longer duplicates!
            FROM temp_user_remapping m
            WHERE u.user_id = m.original_user_id 
              AND u.creation_date = m.creation_date
              AND u.possible_duplicate = TRUE; -- Only update the flagged ones
        """)

        # ==============================================================================
        # STEP 3: FIX NAME-BASED TABLES (Job, Credit Card)
        # We match on ID + NAME to ensure we assign the right "Mariam" vs "Tom"
        # ==============================================================================
        print("3. Updating related tables (Job, Credit Card) using Name matching...")
        
        # 3A. Fix User Jobs
        cur.execute("""
            UPDATE stg_user_job j
            SET user_id = m.new_user_id
            FROM temp_user_remapping m
            WHERE j.user_id = m.original_user_id 
              AND j.name = m.name
              AND m.new_user_id != m.original_user_id; -- Only update if ID changed
        """)

        # 3B. Fix Credit Cards
        cur.execute("""
            UPDATE stg_user_credit_card c
            SET user_id = m.new_user_id
            FROM temp_user_remapping m
            WHERE c.user_id = m.original_user_id 
              AND c.name = m.name
              AND m.new_user_id != m.original_user_id;
        """)

        # ==============================================================================
        # STEP 4: FIX DATE-BASED TABLES (Orders)
        # Orders don't have names, so we match based on "Who was active at this time?"
        # ==============================================================================
        print("4. Updating Orders table using Date Window logic...")
        
        cur.execute("""
            UPDATE stg_order_data o
            SET user_id = m.new_user_id
            FROM temp_user_remapping m
            WHERE o.user_id = m.original_user_id
              -- The order must happen AFTER this user version was created...
              AND o.transaction_date >= m.valid_from
              -- ...and BEFORE the next user version took over (or NULL if this is the latest)
              AND (o.transaction_date < m.valid_to OR m.valid_to IS NULL)
              AND m.new_user_id != m.original_user_id;
        """)

        # ==============================================================================
        # STEP 5: CLEANUP
        # ==============================================================================
        print("5. Cleaning up...")
        cur.execute("DROP TABLE temp_user_remapping;")
        
        conn.commit()
        print("✅ User ID Collision Resolution Complete!")
        
        return {"status": "success", "message": "Historical IDs generated and propagated."}

    except Exception as e:
        conn.rollback()
        print(f"❌ Error during transformation: {e}")
        raise e
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()