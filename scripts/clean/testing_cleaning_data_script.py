import os
import wmill
import pandas as pd
from sqlalchemy import create_engine, text
import logging
import psycopg2

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database Connection Config
# TODO: Update with your actual database credentials
DB_CONFIG = {
    "user": "postgres",
    "password": "shopzada",
    "host": "db",
    "port": "5432",
    "dbname": "shopzada"
}

# Allow overriding via environment variables
DB_USER = os.getenv("DB_USER", DB_CONFIG["user"])
DB_PASSWORD = os.getenv("DB_PASSWORD", DB_CONFIG["password"])
DB_HOST = os.getenv("DB_HOST", DB_CONFIG["host"])
DB_PORT = os.getenv("DB_PORT", DB_CONFIG["port"])
DB_NAME = os.getenv("DB_NAME", DB_CONFIG["dbname"])

DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def get_db_engine():
    """Create and return a SQLAlchemy engine."""
    try:
        engine = create_engine(DATABASE_URI)
        # Test connection
        with engine.connect() as conn:
            pass
        return engine
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        raise

def recreate_tables(engine):
    """Drop and recreate dimension tables."""
    logging.info("Recreating dimension tables...")
    
    # DDL for dim_user
    ddl_dim_user = """
    CREATE TABLE dim_user (
        user_key BIGINT PRIMARY KEY,
        source_user_id TEXT NOT NULL,
        name TEXT,
        creation_date DATE,
        valid_from DATE,
        valid_to DATE,
        is_current BOOLEAN
    );
    """
    
    # DDL for dim_merchant
    ddl_dim_merchant = """
    CREATE TABLE dim_merchant (
        merchant_key BIGINT PRIMARY KEY,
        source_merchant_id TEXT NOT NULL,
        name TEXT,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN
    );
    """
    
    # DDL for dim_staff
    ddl_dim_staff = """
    CREATE TABLE dim_staff (
        staff_key BIGINT PRIMARY KEY,
        source_staff_id TEXT NOT NULL,
        name TEXT,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN
    );
    """
    
    with engine.connect() as conn:
        # Drop existing tables
        conn.execute(text("DROP TABLE IF EXISTS dim_user CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_merchant CASCADE;"))
        conn.execute(text("DROP TABLE IF EXISTS dim_staff CASCADE;"))
        
        # Create tables
        conn.execute(text(ddl_dim_user))
        conn.execute(text(ddl_dim_merchant))
        conn.execute(text(ddl_dim_staff))
        conn.commit()
    
    logging.info("Tables recreated successfully.")

def extract_data(engine, table_name):
    """Read data from a postgres table into a DataFrame."""
    logging.info(f"Extracting data from {table_name}...")
    try:
        query = f"SELECT * FROM {table_name}"
        return pd.read_sql(query, engine)
    except Exception as e:
        logging.error(f"Error extracting {table_name}: {e}")
        return pd.DataFrame()

def clean_and_deduplicate(df, id_col):
    """
    Clean data and remove duplicates based on the 'possible_duplicate' flag.
    
    Strategy:
    1. If 'possible_duplicate' is True, drop the row.
    2. Drop rows with missing source IDs.
    3. Remove strict duplicates on the source ID.
    """
    if df.empty:
        return df

    initial_count = len(df)
    
    # Check if duplication flags exist
    if 'possible_duplicate' in df.columns:
        # Keep only non-duplicates
        # We assume False, None, or 0 means it's a valid unique record
        # Convert to boolean just in case
        is_duplicate = df['possible_duplicate'].astype(str).str.lower() == 'true'
        df_clean = df[~is_duplicate].copy()
        
        duplicates_removed = initial_count - len(df_clean)
        if duplicates_removed > 0:
            logging.info(f"Removed {duplicates_removed} records marked as 'possible_duplicate'.")
    else:
        df_clean = df.copy()

    # Ensure source ID is present
    df_clean = df_clean.dropna(subset=[id_col])
    
    # Remove strict duplicates on the source ID just in case (keep first)
    before_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=[id_col], keep='first')
    dedup_count = before_dedup - len(df_clean)
    
    if dedup_count > 0:
        logging.info(f"Removed {dedup_count} strict duplicates on {id_col}.")
    
    return df_clean

def assign_surrogate_keys(new_data_df, existing_dim_df, sk_col, source_id_col):
    """
    Assign surrogate keys to new data using the Max + 1 method.
    """
    if new_data_df.empty:
        return pd.DataFrame()

    if existing_dim_df.empty:
        start_sk = 1
        existing_ids = set()
    else:
        # Handle case where table exists but is empty or sk_col is null
        max_sk = existing_dim_df[sk_col].max()
        start_sk = 1 if pd.isna(max_sk) else int(max_sk) + 1
        existing_ids = set(existing_dim_df[source_id_col].dropna())
    
    # Identify new records that aren't in the existing dimension
    new_records_mask = ~new_data_df[source_id_col].isin(existing_ids)
    records_to_add = new_data_df[new_records_mask].copy()
    
    if records_to_add.empty:
        logging.info("No new records to add.")
        return pd.DataFrame()
    
    # Assign keys
    records_to_add[sk_col] = range(start_sk, start_sk + len(records_to_add))
    
    logging.info(f"Assigned {len(records_to_add)} new surrogate keys starting from {start_sk}.")
    
    return records_to_add

def process_dimension_users(engine):
    """ETL Process for User Dimension."""
    logging.info("--- Processing Dimension: Users ---")
    
    # 1. Extract
    stg_users = extract_data(engine, 'stg_user_data')
    dim_users_existing = extract_data(engine, 'dim_user')
    
    if stg_users.empty:
        logging.warning("Staging table stg_user_data is empty.")
        return

    # 2. Transform (Clean)
    clean_users = clean_and_deduplicate(stg_users, 'user_id')
    
    # 3. Transform (Surrogate Keys)
    # Map staging columns to dimension columns
    # stg: user_id, name, creation_date, ...
    # dim: user_key, source_user_id, name, creation_date, valid_from, valid_to, is_current
    
    clean_users.rename(columns={'user_id': 'source_user_id'}, inplace=True)
    
    new_dim_records = assign_surrogate_keys(
        clean_users, 
        dim_users_existing, 
        sk_col='user_key', 
        source_id_col='source_user_id'
    )
    
    # 4. Load
    if not new_dim_records.empty:
        # Select and order columns to match destination
        # We need to ensure we only try to insert columns that exist in the target or handle defaults
        
        # Set SCD Type 2 fields (simplification: just inserting as current)
        new_dim_records['is_current'] = True
        new_dim_records['valid_from'] = pd.Timestamp.now().date()
        new_dim_records['valid_to'] = None
        
        # Filter columns to only those present in dim_user (if we knew them strictly)
        # For now, we'll assume the dataframe has extra columns that SQL might reject if not careful,
        # but pandas to_sql usually handles matching names.
        # We should strictly select columns based on the dimension definition.
        
        target_columns = ['user_key', 'source_user_id', 'name', 'creation_date', 'valid_from', 'valid_to', 'is_current']
        
        # Ensure all target cols exist (fill missing with None)
        for col in target_columns:
            if col not in new_dim_records.columns:
                new_dim_records[col] = None
        
        final_df = new_dim_records[target_columns]
        
        logging.info(f"Ready to insert {len(final_df)} rows into dim_user.")
        
        # In a real run, uncomment the following line:
        final_df.to_sql('dim_user', engine, if_exists='append', index=False)
        
        # For preview:
        print(final_df.head())
    else:
        logging.info("Dimension is up to date.")

def process_dimension_merchants(engine):
    """ETL Process for Merchant Dimension."""
    logging.info("--- Processing Dimension: Merchants ---")
    
    stg_merch = extract_data(engine, 'stg_merchant_data')
    dim_merch_existing = extract_data(engine, 'dim_merchant')
    
    if stg_merch.empty:
        logging.warning("Staging table stg_merchant_data is empty.")
        return
    
    clean_merch = clean_and_deduplicate(stg_merch, 'merchant_id')
    clean_merch.rename(columns={'merchant_id': 'source_merchant_id'}, inplace=True)
    
    new_dim_records = assign_surrogate_keys(
        clean_merch,
        dim_merch_existing,
        sk_col='merchant_key',
        source_id_col='source_merchant_id'
    )
    
    if not new_dim_records.empty:
        new_dim_records['is_current'] = True
        new_dim_records['valid_from'] = pd.Timestamp.now() # timestamp for merchant
        new_dim_records['valid_to'] = None
        
        target_columns = ['merchant_key', 'source_merchant_id', 'name', 'valid_from', 'valid_to', 'is_current']
        
        for col in target_columns:
            if col not in new_dim_records.columns:
                new_dim_records[col] = None
                
        final_df = new_dim_records[target_columns]
        
        logging.info(f"Ready to insert {len(final_df)} rows into dim_merchant.")
        final_df.to_sql('dim_merchant', engine, if_exists='append', index=False)
        print(final_df.head())
    else:
        logging.info("Dimension is up to date.")

def process_dimension_staff(engine):
    """ETL Process for Staff Dimension."""
    logging.info("--- Processing Dimension: Staff ---")
    
    stg_staff = extract_data(engine, 'stg_staff_data')
    dim_staff_existing = extract_data(engine, 'dim_staff')
    
    if stg_staff.empty:
        logging.warning("Staging table stg_staff_data is empty.")
        return
    
    clean_staff = clean_and_deduplicate(stg_staff, 'staff_id')
    clean_staff.rename(columns={'staff_id': 'source_staff_id'}, inplace=True)
    
    new_dim_records = assign_surrogate_keys(
        clean_staff,
        dim_staff_existing,
        sk_col='staff_key',
        source_id_col='source_staff_id'
    )
    
    if not new_dim_records.empty:
        new_dim_records['is_current'] = True
        new_dim_records['valid_from'] = pd.Timestamp.now()
        new_dim_records['valid_to'] = None
        
        target_columns = ['staff_key', 'source_staff_id', 'name', 'valid_from', 'valid_to', 'is_current']
        
        for col in target_columns:
            if col not in new_dim_records.columns:
                new_dim_records[col] = None
                
        final_df = new_dim_records[target_columns]
        
        logging.info(f"Ready to insert {len(final_df)} rows into dim_staff.")
        final_df.to_sql('dim_staff', engine, if_exists='append', index=False)
        print(final_df.head())
    else:
        logging.info("Dimension is up to date.")

def main():
    try:
        engine = get_db_engine()
        
        recreate_tables(engine)
        
        process_dimension_users(engine)
        process_dimension_merchants(engine)
        process_dimension_staff(engine)
        
        logging.info("ETL Pipeline completed successfully.")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")