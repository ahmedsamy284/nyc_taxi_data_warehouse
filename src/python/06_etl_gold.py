"""
Gold Layer ETL Pipeline Module
------------------------------

This module orchestrates the extraction, transformation, and loading (ETL) of data 
from the Silver layer into the final Star Schema (Gold layer) data warehouse. 
It processes dimension tables (date, location, weather) and a central fact table (taxi trips),
ensuring referential integrity, memory optimization, and proper identity resets.
"""

# To track pipeline execution events, monitor progress, and record errors
import logging

# To perform fast, vectorized data manipulation and transformations (DataFrames)
import pandas as pd

# To safely wrap raw SQL queries for execution via SQLAlchemy engine
from sqlalchemy import text

# To provide the centralized file path for saving Gold layer logs
from config import gold_log_path

# To reuse the singleton database connection and standardized logging setup
from common_utils import setup_logging, engine


# ==============================================================================
# 1. Pipeline Initialization
# ==============================================================================

# Initialize logging configuration using the centralized shared setup
setup_logging(gold_log_path)

# Visual separator for log readability indicating the start of the Gold ETL phase
logging.info("\n" + "="*50 + "\n=== Starting ETL process for Gold layer ===\n" + "="*50)


# ==============================================================================
# 2. Data Extraction Phase (Extract)
# ==============================================================================

def extract_from_silver(table_name, engine):
    """
    Extracts all records from a specified Silver layer table into a pandas DataFrame.

    Args:
        table_name (str): The name of the table in the Silver schema.
        engine (sqlalchemy.engine.Engine): The database connection engine.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted Silver layer data.
    """
    try:
        logging.info(f"Extracting data from silver.{table_name}...")
        
        # Construct the SQL query to fetch all data from the specific silver table
        query = text(f"SELECT * FROM silver.{table_name}")
        
        # Execute the query and load results into a pandas DataFrame
        df = pd.read_sql(query, engine, index_col=None)
        
        logging.info(f"Successfully extracted {len(df)} records from silver.{table_name}.")
        return df

    except Exception as e:
        logging.error(f"Failed to extract data from silver.{table_name}. Error: {e}")
        raise


def fetch_surrogate_keys(dim_table, natural_key_col, surrogate_key_col, engine):
    """
    Retrieves surrogate keys and their corresponding natural keys from a Gold dimension table.
    This acts as a lookup table to map facts to their respective dimensions.

    Args:
        dim_table (str): The Gold dimension table to fetch keys from.
        natural_key_col (str): The column name of the business/natural key.
        surrogate_key_col (str): The column name of the generated surrogate key.
        engine (sqlalchemy.engine.Engine): The database connection engine.

    Returns:
        pd.DataFrame: A DataFrame containing the surrogate and natural key mappings.
    """
    try:
        logging.info(f"Fetching lookup mapping ({surrogate_key_col}, {natural_key_col}) from {dim_table}...")
        
        # Construct query to retrieve only the necessary key columns for the merge
        query = text(f"SELECT {surrogate_key_col}, {natural_key_col} FROM gold.{dim_table}")
        df = pd.read_sql(query, engine, index_col=None)
        
        logging.info(f"Successfully retrieved {len(df)} keys from gold.{dim_table}.")
        return df

    except Exception as e:
        logging.error(f"Failed to fetch keys from gold.{dim_table}. Error: {e}")
        raise


# ==============================================================================
# 3. Data Transformation Phase (Transform)
# ==============================================================================

def transform_dim_date_gold(df, table_name):
    """
    Transforms the date dimension data to match the Gold layer schema.

    Args:
        df (pd.DataFrame): The raw date DataFrame from the Silver layer.
        table_name (str): The target table name for logging purposes.

    Returns:
        pd.DataFrame: The transformed date dimension DataFrame.
    """
    try:
        logging.info(f"Transforming dimension data for {table_name}...")
        
        # Rename columns to match the standardized Gold schema requirements
        renamed_columns = {
            'day_of_week': 'day_of_week_num', 
            'is_weekend': 'is_weekend_flag', 
            'is_holiday': 'is_holiday_flag'
        }
        df = df.rename(columns=renamed_columns)

        logging.info(f"Successfully transformed dimension data for {table_name}.")
        return df

    except Exception as e:
        logging.error(f"Failed to transform dimension data for {table_name}. Error: {e}")
        raise


def transform_dim_location_gold(df, table_name):
    """
    Transforms the location dimension data to match the Gold layer schema.

    Args:
        df (pd.DataFrame): The raw location DataFrame from the Silver layer.
        table_name (str): The target table name for logging purposes.

    Returns:
        pd.DataFrame: The transformed location dimension DataFrame.
    """
    try:
        logging.info(f"Transforming dimension data for {table_name}...")

        # Standardize naming conventions for location attributes
        renamed_columns = {
            'borough': 'borough_name', 
            'zone_name': 'taxi_zone_name'
        }
        df = df.rename(columns=renamed_columns)

        logging.info(f"Successfully transformed dimension data for {table_name}.")
        return df

    except Exception as e:
        logging.error(f"Failed to transform dimension data for {table_name}. Error: {e}")
        raise


def transform_dim_weather_gold(df, table_name):
    """
    Transforms the weather dimension data, filtering unnecessary columns to match the Gold schema.

    Args:
        df (pd.DataFrame): The raw weather DataFrame from the Silver layer.
        table_name (str): The target table name for logging purposes.

    Returns:
        pd.DataFrame: The transformed weather dimension DataFrame.
    """
    try:
        logging.info(f"Transforming dimension data for {table_name}...")

        # Rename columns to be more descriptive and business-friendly
        renamed_columns = {
            'full_datetime': 'weather_date', 
            'temp': 'temperature_celsius',
            'preciptype': 'precipitation_type',
            'snowdepth': 'snow_depth_cm',
            'windspeed': 'wind_speed_kmh',
            'conditions': 'weather_condition_desc'
        }

        # Drop operational columns that hold no analytical value for the Gold layer
        dropped_columns = [
            'date_time', 'day_datetime', 'feelslike', 
            'precip', 'snow', 'windgust', 'visibility', 'icon'
        ]

        df = df.rename(columns=renamed_columns)
        df = df.drop(columns=dropped_columns)

        logging.info(f"Successfully transformed dimension data for {table_name}.")
        return df

    except Exception as e:
        logging.error(f"Failed to transform dimension data for {table_name}. Error: {e}")
        raise


def transform_fact_trips_gold(df, table_name, date_keys_df, loc_keys_df, weather_keys_df):
    """
    Transforms the main trips fact table by joining it with dimension lookup tables
    to replace natural keys with surrogate keys, applying final column formatting.

    Args:
        df (pd.DataFrame): The raw taxi trips DataFrame from the Silver layer.
        table_name (str): The target fact table name for logging purposes.
        date_keys_df (pd.DataFrame): Lookup DataFrame for date dimension keys.
        loc_keys_df (pd.DataFrame): Lookup DataFrame for location dimension keys.
        weather_keys_df (pd.DataFrame): Lookup DataFrame for weather dimension keys.

    Returns:
        pd.DataFrame: The final, transformed fact table ready for the Gold layer.
    """
    try:
        logging.info(f"Starting transformations and key mapping for {table_name}...")
        logging.info(f"Initial row count from Silver: {len(df)}")

        # Extract hour and normalize datetimes to match dimension date formats for merging
        df['pickup_hour'] = df['pickup_datetime'].dt.hour
        df['pickup_date_temp'] = df['pickup_datetime'].dt.normalize()
        df['dropoff_date_temp'] = df['dropoff_datetime'].dt.normalize()

        # Merge with Location Dimension (Pickup)
        df = pd.merge(df, loc_keys_df, how='inner', left_on='pulocation_id', right_on='location_id')
        df = df.rename(columns={'location_key': 'pickup_location_key', 'location_id': 'location_id_pickup'})
        logging.info(f"Row count after Pickup Location merge: {len(df)}")

        # Merge with Location Dimension (Dropoff)
        df = pd.merge(df, loc_keys_df, how='inner', left_on='dolocation_id', right_on='location_id')
        df = df.rename(columns={'location_key': 'dropoff_location_key', 'location_id': 'location_id_dropoff'})
        logging.info(f"Row count after Dropoff Location merge: {len(df)}")

        # Merge with Date Dimension (Pickup)
        df = pd.merge(df, date_keys_df, how='inner', left_on='pickup_date_temp', right_on='full_date')
        df = df.rename(columns={'date_key': 'pickup_date_key', 'full_date': 'full_date_pickup'})
        logging.info(f"Row count after Pickup Date merge: {len(df)}")

        # Merge with Date Dimension (Dropoff)
        df = pd.merge(df, date_keys_df, how='inner', left_on='dropoff_date_temp', right_on='full_date')
        df = df.rename(columns={'date_key': 'dropoff_date_key', 'full_date': 'full_date_dropoff'})
        logging.info(f"Row count after Dropoff Date merge: {len(df)}")

        # Merge with Weather Dimension (Based on Pickup Date)
        df = pd.merge(df, weather_keys_df, how='inner', left_on='pickup_date_temp', right_on='weather_date')
        logging.info(f"Row count after Weather merge: {len(df)}")

        # Format financial and metric columns to align with business terminology
        columns_to_rename = {
            'trip_distance': 'trip_distance_miles',
            'fare_amount': 'fare_amount_usd',
            'tip_amount': 'tip_amount_usd',
            'total_amount': 'total_amount_usd'
        }
        df = df.rename(columns=columns_to_rename)

        # Select only the required columns for the final Star Schema fact table
        final_columns = [
            'pickup_date_key', 'dropoff_date_key',
            'pickup_location_key', 'dropoff_location_key', 'weather_key',
            'vendor_id', 'pickup_hour', 'passenger_count',
            'trip_distance_miles', 'fare_amount_usd', 'tip_amount_usd', 'total_amount_usd'
        ]        
        df = df[final_columns]

        logging.info(f"Successfully joined all surrogate keys and formatted metrics for {table_name}.")
        return df

    except Exception as e:
        logging.error(f"Failed to transform data for {table_name}. Error: {e}")
        raise


# ==============================================================================
# 4. Data Cleanup Phase (Truncate)
# ==============================================================================

def truncate_gold_tables(engine):
    """
    Safely truncates all Gold tables by temporarily dropping Foreign Key constraints,
    truncating the tables to reset identity counters, and then recreating the constraints.
    This ensures a clean slate for the ETL load without violating referential integrity.

    Args:
        engine (sqlalchemy.engine.Engine): The database connection engine.
    """
    try:
        logging.info("Starting Pre-Load Cleanup: Dropping FKs, Truncating, and Recreating FKs...")

        # Use engine.begin() to manage the transaction automatically (commit on success, rollback on failure)
        with engine.begin() as conn:
            
            # Step 1: Drop Foreign Key Constraints
            logging.info("Step 1: Dropping Foreign Key constraints from fact_taxi_trips...")
            
            drop_fks_query = """
                ALTER TABLE gold.fact_taxi_trips DROP CONSTRAINT IF EXISTS fk_fact_pickup_date;
                ALTER TABLE gold.fact_taxi_trips DROP CONSTRAINT IF EXISTS fk_fact_dropoff_date;
                ALTER TABLE gold.fact_taxi_trips DROP CONSTRAINT IF EXISTS fk_fact_pickup_location;
                ALTER TABLE gold.fact_taxi_trips DROP CONSTRAINT IF EXISTS fk_fact_dropoff_location;
                ALTER TABLE gold.fact_taxi_trips DROP CONSTRAINT IF EXISTS fk_fact_weather;
            """
            conn.execute(text(drop_fks_query))
            
            # Step 2: Truncate Tables (Safe to do now as constraints are removed)
            logging.info("Step 2: Truncating Gold tables to reset identity counters...")
            
            gold_tables = ['fact_taxi_trips', 'dim_date', 'dim_location', 'dim_weather']
            for table_name in gold_tables:
                conn.execute(text(f"TRUNCATE TABLE gold.{table_name}"))
                logging.info(f"Table gold.{table_name} truncated successfully.")

        logging.info("Pre-Load Cleanup completed successfully. All Gold tables are empty.")

    except Exception as e:
        logging.error(f"Failed during Gold tables Truncation. Error: {e}")
        raise


# ==============================================================================
# 5. Data Loading Phase (Load)
# ==============================================================================

def load_to_gold(df, table_name, engine):
    """
    Appends a cleaned DataFrame into the specified Gold layer table.

    Args:
        df (pd.DataFrame): The transformed data ready for loading.
        table_name (str): The target table name in the Gold schema.
        engine (sqlalchemy.engine.Engine): The database connection engine.
    """
    try:      
        logging.info(f"Loading data into gold.{table_name}...")
        
        # Append data in chunks to optimize memory and database performance
        df.to_sql(
            name=table_name,
            schema="gold",
            con=engine,
            if_exists='append',
            index=False,
            chunksize=10000
        )
        
        logging.info(f"Successfully loaded {len(df)} records into gold.{table_name}.")

    except Exception as e:
        logging.error(f"Failed to load data into gold.{table_name}. Error: {e}")
        raise


# ==============================================================================
# 6. Constraint Management Phase
# ==============================================================================

def recreate_gold_fks(engine):
    """
    Recreates the Foreign Key constraints on the fact_taxi_trips table after the data load.
    
    Applying constraints AFTER the bulk data insertion significantly improves ETL performance 
    by avoiding row-by-row referential integrity checks during the load process.

    Args:
        engine (sqlalchemy.engine.Engine): The database connection engine.
    """
    try:
        logging.info("Starting Constraint Management Phase...")
        logging.info("Recreating Foreign Key constraints on gold.fact_taxi_trips...")

        # Use engine.begin() to manage the transaction automatically
        with engine.begin() as conn:
            add_fks_query = """
                ALTER TABLE gold.fact_taxi_trips 
                    ADD CONSTRAINT fk_fact_pickup_date FOREIGN KEY (pickup_date_key) REFERENCES gold.dim_date(date_key);
                    
                ALTER TABLE gold.fact_taxi_trips 
                    ADD CONSTRAINT fk_fact_dropoff_date FOREIGN KEY (dropoff_date_key) REFERENCES gold.dim_date(date_key);
                    
                ALTER TABLE gold.fact_taxi_trips 
                    ADD CONSTRAINT fk_fact_pickup_location FOREIGN KEY (pickup_location_key) REFERENCES gold.dim_location(location_key);
                    
                ALTER TABLE gold.fact_taxi_trips 
                    ADD CONSTRAINT fk_fact_dropoff_location FOREIGN KEY (dropoff_location_key) REFERENCES gold.dim_location(location_key);
                    
                ALTER TABLE gold.fact_taxi_trips 
                    ADD CONSTRAINT fk_fact_weather FOREIGN KEY (weather_key) REFERENCES gold.dim_weather(weather_key);
            """
            conn.execute(text(add_fks_query))

        logging.info("Successfully recreated all Foreign Key constraints on gold.fact_taxi_trips.")

    except Exception as e:
        logging.error(f"Failed to recreate Foreign Key constraints. Error: {e}")
        raise

# ==============================================================================
# 7. Pipeline Orchestration
# ==============================================================================

def orchestrator_gold():
    """
    The main orchestrator function that dictates the flow of the Gold layer ETL process.
    It manages pre-load cleanup, dimension processing, fact table processing, 
    and strict memory management.
    """
    
    # 0. Pre-Load Cleanup
    truncate_gold_tables(engine)

    # 1. Process Date Dimension
    date_df = extract_from_silver('dim_date', engine)
    date_df = transform_dim_date_gold(date_df, 'dim_date')
    load_to_gold(date_df, 'dim_date', engine)

    # 2. Process Location Dimension
    loc_df = extract_from_silver('taxi_location', engine)
    loc_df = transform_dim_location_gold(loc_df, 'taxi_location')
    load_to_gold(loc_df, 'dim_location', engine)

    # 3. Process Weather Dimension
    weather_df = extract_from_silver('weather', engine)
    weather_df = transform_dim_weather_gold(weather_df, 'weather')
    load_to_gold(weather_df, 'dim_weather', engine)

    # 4. Fetch Surrogate Keys for Fact Table Mapping
    date_keys_df = fetch_surrogate_keys('dim_date', 'full_date', 'date_key', engine)
    # Ensure correct datetime format for subsequent pandas merging
    date_keys_df['full_date'] = pd.to_datetime(date_keys_df['full_date'], errors='coerce')
    
    loc_keys_df = fetch_surrogate_keys('dim_location', 'location_id', 'location_key', engine)
    weather_keys_df = fetch_surrogate_keys('dim_weather', 'weather_date', 'weather_key', engine)

    # 5. Process Fact Table (Overwriting DataFrame to optimize RAM usage)
    trips_df = extract_from_silver('taxi_trips', engine)
    trips_df = transform_fact_trips_gold(trips_df, 'taxi_trips', date_keys_df, loc_keys_df, weather_keys_df)
    load_to_gold(trips_df, 'fact_taxi_trips', engine)

    # 6. Constraint Management (Re-apply FKs after load for performance)
    recreate_gold_fks(engine)

    # 7. Memory Cleanup (Freeing up RAM resources)
    del date_df, loc_df, weather_df, date_keys_df, loc_keys_df, weather_keys_df, trips_df


# ==============================================================================
# 8. Script Entry Point
# ==============================================================================

if __name__ == "__main__":
    orchestrator_gold()

    # Provide immediate visual feedback in the console upon successful execution,
    # directing the user to the dedicated log file for detailed execution history.
    print("ETL process for Gold layer completed successfully. Check the log file for details.")