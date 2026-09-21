"""
Silver Layer ETL Pipeline - NYC Taxi Data Warehouse
---------------------------------------------------
This script is responsible for extracting raw data from the Bronze layer (SQL Server),
applying business rules, data cleansing, and data type transformations (Silver layer processing),
and loading the cleansed data into the Silver schema.

Design Pattern: Metadata-Driven ETL utilizing a configuration dictionary for dynamic orchestration.
"""

# To track pipeline execution events, monitor progress, and record errors
import logging

# To perform fast, vectorized data manipulation and transformations (DataFrames)
import pandas as pd

# To safely wrap raw SQL queries for execution via SQLAlchemy engine
from sqlalchemy import text

# To provide the centralized file path for saving Silver layer logs
from config import silver_log_path

# To reuse the singleton database connection and standardized logging setup
from common_utils import setup_logging, engine


# ==============================================================================
# 1. Pipeline Initialization
# ==============================================================================

# Initialize logging configuration using the centralized shared setup
setup_logging(silver_log_path)

# Visual separator for log readability indicating the start of the Silver ETL phase
logging.info("\n" + "="*50 + "\n=== Starting ETL process for Silver layer ===\n" + "="*50)


# ==============================================================================
# 2. Data Extraction Phase (Extract)
# ==============================================================================

def extract_from_bronze(table_name, engine):
    """
    Extracts all records from a specified table in the Bronze schema.

    Args:
        table_name (str): The name of the table to extract from the bronze schema.
        engine (sqlalchemy.engine.Engine): The SQLAlchemy database connection engine.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the extracted raw data.
    """
    try:
        logging.info(f"Starting Extract data from bronze.{table_name}...")
        
        # Construct the SQL query to fetch all data from the specific bronze table
        query = text(f"SELECT * FROM bronze.{table_name}")
        
        # Execute the query and load results into a pandas DataFrame
        df = pd.read_sql(query, engine, index_col=None)
        
        logging.info(f"Successfully extracted {len(df)} records from bronze.{table_name}.")
        return df

    except Exception as e:
        logging.error(f"Failed to extract data from bronze.{table_name}. Error: {e}")
        raise


# ==============================================================================
# 3. Data Transformation Phase (T) - Dimension Tables
# ==============================================================================

def transform_dim_date(df, table_name):
    """
    Cleanses and transforms the Date Dimension table.
    
    Operations:
    - Removes duplicate records.
    - Fills missing values with 'Unknown'.
    - Casts date strings to datetime objects.
    - Standardizes text casing (Title Case).
    - Converts boolean/flag columns to integers for database compatibility.

    Args:
        df (pd.DataFrame): The raw date DataFrame.
        table_name (str): Target table name (used for logging).

    Returns:
        pd.DataFrame: Cleansed date DataFrame.
    """
    try:
        logging.info(f"Transforming data for {table_name}...")

        # Remove duplicate rows based on the primary key (full_date) 
        df = df.drop_duplicates(subset=['full_date'], keep='first')
        
        # Handle Null values globally by replacing them with 'Unknown'
        df = df.fillna("Unknown")
        
        # Ensure the primary date column is cast to datetime datatype
        df['full_date'] = pd.to_datetime(df['full_date'], errors='coerce')
        
        # Standardize categorical text to Title Case
        df['day_name'] = df['day_name'].astype(str).str.title()
        
        # Convert numerical and boolean flag columns to explicit integers (downcasted for memory efficiency)
        df['day_of_week'] = pd.to_numeric(df['day_of_week'], errors='coerce', downcast='integer')
        df['is_weekend'] = pd.to_numeric(df['is_weekend'], errors='coerce', downcast='integer')
        df['is_holiday'] = pd.to_numeric(df['is_holiday'], errors='coerce', downcast='integer')

        logging.info(f"Successfully transformed data for {table_name}.")
        return df

    except Exception as e:
        logging.error(f"Failed to transform data for {table_name}. Error: {e}")
        raise


def transform_taxi_location(df, table_name):
    """
    Cleanses and transforms the Taxi Location Dimension table.

    Operations:
    - Removes duplicate rows.
    - Drops rows with a missing primary key (location_id).
    - Fills missing text data with 'Unknown'.
    - Standardizes categorical text data to Title Case.

    Args:
        df (pd.DataFrame): The raw location DataFrame.
        table_name (str): Target table name (used for logging).

    Returns:
        pd.DataFrame: Cleansed location DataFrame.
    """
    try:
        logging.info(f"Transforming data for {table_name}...")

        # Remove duplicate rows keeping the first occurrence
        df = df.drop_duplicates(subset=['location_id'], keep='first')
        
        # Drop rows where the primary key (location_id) is missing to maintain referential integrity
        df = df.dropna(subset=['location_id'])
        
        # Handle remaining missing values in categorical columns
        df = df.fillna('Unknown')
        
        # Ensure location_id is strictly an integer
        df['location_id'] = pd.to_numeric(df['location_id'], errors='coerce', downcast='integer')
        
        # Apply Title Case standardization to all geographical text columns
        df['borough'] = df['borough'].astype(str).str.title()
        df['zone_name'] = df['zone_name'].astype(str).str.title()
        df['service_zone'] = df['service_zone'].astype(str).str.title()

        logging.info(f"Successfully transformed data for {table_name}.")
        return df

    except Exception as e:
        logging.error(f"Failed to transform data for {table_name}. Error: {e}")
        raise


def transform_weather(df, table_name):
    """
    Cleanses and transforms the Weather Dimension table.

    Operations:
    - Creates a unified 'full_datetime' key for temporal joins.
    - Cleans malformed precipitation type strings.
    - Fills missing numeric measures (snow, rain) with 0.
    - Fills categorical missing data with 'Unknown'.
    - Enforces float datatypes for meteorological measures using a fast loop.

    Args:
        df (pd.DataFrame): The raw weather DataFrame.
        table_name (str): Target table name (used for logging).

    Returns:
        pd.DataFrame: Cleansed weather DataFrame.
    """
    try:
        logging.info(f"Transforming data for {table_name}...")

        # Drop exact duplicate rows
        df = df.drop_duplicates(keep='first')
        
        # Construct a unified timestamp column at position 0 to facilitate future SQL JOIN operations
        df.insert(loc=0, 
                  column='full_datetime', 
                  value=(pd.to_datetime(df['day_datetime'].astype(str) + ' ' + df['date_time'].astype(str), errors='coerce')))
        
        # Cast the date column to pandas datetime
        df['day_datetime'] = pd.to_datetime(df['day_datetime'], errors='coerce') 
        
        # Clean the 'preciptype' column by stripping out JSON-like brackets and quotes
        df['preciptype'] = df['preciptype'].str.replace(r"\[|\]|'","", regex=True)
        
        # Logical NULL handling: if preciptype is missing, assume 'None'
        df['preciptype'] = df['preciptype'].fillna("None")
        
        # Logical NULL handling: if text conditions are missing, mark as 'Unknown'
        df[['conditions', 'icon']] = df[['conditions', 'icon']].fillna("Unknown")
        
        # Logical NULL handling: if rain/snow amounts are missing, assume 0
        df[['precip', 'snow', 'snowdepth', 'windgust']] = df[['precip', 'snow', 'snowdepth', 'windgust']].fillna(0)
        
        # Standardize casing for categorical columns
        text_columns = ['preciptype', 'conditions', 'icon']
        for col in text_columns:
            df[col] = df[col].astype(str).str.title()
        
        # Ensure all measurement columns are properly cast to numeric types (floats)
        numeric_columns = ['temp', 'feelslike', 'precip', 'snow', 'snowdepth', 'windspeed', 'windgust', 'visibility']
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        logging.info(f"Successfully transformed data for {table_name}.")
        return df

    except Exception as e:
        logging.error(f"Failed to transform data for {table_name}. Error: {e}")
        raise


# ==============================================================================
# 4. Data Transformation Phase (T) - Fact Table
# ==============================================================================

def transform_taxi_trips(df, table_name):
    """
    Cleanses and transforms the Fact Taxi Trips table.

    Operations:
    - Drops duplicate rows.
    - Drops any corrupted transaction (any row containing NULLs).
    - Enforces strict data types (integers for IDs, floats for financial data).
    - Applies strict Business Rules (Time logic, Scope logic, Positive value constraints).

    Args:
        df (pd.DataFrame): The raw taxi trips DataFrame.
        table_name (str): Target table name (used for logging).

    Returns:
        pd.DataFrame: Cleansed and validated Fact DataFrame.
    """
    try:
        logging.info(f"Transforming data for {table_name}...")

        # Define columns for explicit type casting
        int_columns = ['vendor_id', 'passenger_count', 'pulocation_id', 'dolocation_id']
        float_columns = ['trip_distance', 'fare_amount', 'tip_amount', 'total_amount']

        # Remove complete duplicates
        df = df.drop_duplicates(keep='first')

        # Business Rule: Preserve valid financial transactions missing a passenger count
        # Impute missing (NULL) or zero passenger counts with the logical default of 1
        df['passenger_count'] = df['passenger_count'].fillna(1)
        df['passenger_count'] = df['passenger_count'].replace({0: 1})
        
        # Drop completely corrupted transactions (Any row with at least one NULL value)
        df = df.dropna(how='any')
        logging.info(f"Rows after dropping NULLs: {len(df)}")

        # Apply strict data type casting
        df[int_columns] = df[int_columns].astype(int)
        df[float_columns] = df[float_columns].astype(float)
        
        # Cast timestamps
        df['pickup_datetime'] = pd.to_datetime(df['pickup_datetime'], errors='coerce')
        df['dropoff_datetime'] = pd.to_datetime(df['dropoff_datetime'], errors='coerce')

        # Business Rule 1: Temporal logic and Project Scope (January 2026 only)
        date_filter = ((df['pickup_datetime'] < df['dropoff_datetime']) & 
                       (df['pickup_datetime'] >= pd.to_datetime('2026-01-01')) & 
                       (df['pickup_datetime'] < pd.to_datetime('2026-02-01')))
        df = df[date_filter]
        logging.info(f"Rows after Date Filter: {len(df)}")

        # Business Rule 2: Ensure valid passenger counts, distances, and non-negative financial values
        business_filter = ((df['passenger_count'] > 0) & 
                           (df['trip_distance'] > 0) & 
                           (df['fare_amount'] > 0) & 
                           (df['tip_amount'] >= 0) & 
                           (df['total_amount'] > 0))
        df = df[business_filter]
        logging.info(f"Rows after Business Filter (Passengers, Fares, Distance): {len(df)}")
        
        logging.info(f"Successfully transformed data for {table_name}.")
        return df

    except Exception as e:
        logging.error(f"Failed to transform data for {table_name}. Error: {e}")
        raise


# ==============================================================================
# 5. Data Loading Phase (L)
# ==============================================================================

def load_to_silver(df, table_name, engine):
    """
    Loads the cleansed DataFrame into the designated table in the Silver schema.
    Uses a TRUNCATE and LOAD pattern to maintain idempotency.

    Args:
        df (pd.DataFrame): The cleansed DataFrame ready for loading.
        table_name (str): The target table name in the silver schema.
        engine (sqlalchemy.engine.Engine): The SQLAlchemy database connection engine.
    """
    try:
        # Step 1: Truncate the existing Silver table to prevent data duplication on re-runs
        with engine.connect() as conn:
            truncate_query = text(f"TRUNCATE TABLE silver.{table_name}")
            conn.execute(truncate_query)
            conn.commit()
        logging.info(f"Table silver.{table_name} truncated successfully.")        

        # Step 2: Append the cleansed data into the empty Silver table
        logging.info(f"Loading data into silver.{table_name}...")
        df.to_sql(
            name = table_name,
            schema = "silver",
            con = engine,
            if_exists = 'append',
            index = False,
            chunksize = 100000
        )
        logging.info(f"Successfully loaded {len(df)} records into silver.{table_name}.")

    except Exception as e:
        logging.error(f"Failed to load data into silver.{table_name}. Error: {e}")
        raise


# ==============================================================================
# 6. Orchestrator Silver
# ==============================================================================

def orchestrator_silver():
    """
    Orchestrator Silver function for the Silver layer ETL process.
    Utilizes a metadata dictionary to dynamically extract, transform, and load each table.
    """
    
    # Metadata dictionary mapping Bronze table names to their Silver equivalents and corresponding transformation functions
    etl_config = {
        "date_raw": {
            "silver_name": "dim_date",
            "transform_func": transform_dim_date
        },
        "taxi_location_raw": {
            "silver_name": "taxi_location",
            "transform_func": transform_taxi_location
        },
        "weather_raw": {
            "silver_name": "weather",
            "transform_func": transform_weather
        },
        "taxi_trips_raw": {
            "silver_name": "taxi_trips",
            "transform_func": transform_taxi_trips
        }
    }

    # Iterate through the configuration dictionary to process tables sequentially
    for bronze_table, config in etl_config.items():
        
        silver_table = config["silver_name"]
        transform_function = config["transform_func"]

        # Phase 1: Extract from Bronze
        raw_df = extract_from_bronze(bronze_table, engine)

        # Phase 2: Transform based on specific table rules
        transformed_df = transform_function(raw_df, silver_table)

        # Phase 3: Load into Silver
        load_to_silver(transformed_df, silver_table, engine)

    # Log global completion message once the entire loop finishes successfully
    logging.info("=== ETL process for Silver layer completed successfully ===")

    # 6. Memory Cleanup (Freeing up RAM resources)
    del raw_df, transformed_df

# ==============================================================================
# 7. Script Entry Point
# ==============================================================================

if __name__ == "__main__":
    orchestrator_silver()

    # Provide immediate visual feedback in the console upon successful execution,
    # directing the user to the dedicated log file for detailed execution history.
    print("ETL process for Silver layer completed successfully. Check the log file for details.")