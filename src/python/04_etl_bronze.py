"""
Bronze Layer ETL Ingestion Script
---------------------------------
This script is responsible for executing the ETL pipeline for the Bronze layer 
of the NYC Taxi Data Warehouse. 

It leverages shared utilities for centralized logging and a Singleton database 
connection to enforce the DRY principle. It ingests both CSV dimension tables 
and chunked Parquet fact files into the 'bronze' schema. It ensures idempotency 
by truncating target tables before insertion, and handles dynamic column renaming 
to ensure schema compatibility.
"""

# Importing logging to track pipeline execution events, monitor progress, and record errors.
import logging

# Importing pandas for tabular data manipulation, reading CSV files, and writing DataFrames to SQL Server.
import pandas as pd

# Importing pyarrow.parquet for memory-efficient chunked reading of large Parquet datasets to prevent RAM overload.
import pyarrow.parquet as pq

# Importing text from sqlalchemy to safely construct and execute raw SQL commands (e.g., TRUNCATE TABLE).
from sqlalchemy import text

# Importing dynamically resolved absolute file paths for raw data and logs to maintain the DRY principle.
from config import bronze_log_path, parquet_path, weather_path, date_path, location_path

# Importing the shared logging configurator and the globally instantiated Singleton database engine.
from common_utils import setup_logging, engine


# ==============================================================================
# 1. Pipeline Initialization
# ==============================================================================

# Configure the logging system specifically for the Bronze layer using the shared utility.
setup_logging(bronze_log_path)

# Insert a newline before the starting message to clearly separate distinct execution runs in the log file
logging.info("\n" + "="*50 + "\n=== Starting ETL process for Bronze layer ===\n" + "="*50)


# ==============================================================================
# 2. Data Ingestion Phase (Extract & Load)
# ==============================================================================

def load_csv_to_bronze(file_name, table_name, engine, select_columns=None):
    """
    Loads data from a CSV file into a specified table within the Bronze schema.
    It truncates the existing table prior to insertion to maintain idempotency, 
    and dynamically renames columns to match the target database schema.

    Args:
        file_name (str): The absolute path to the source CSV file.
        table_name (str): The destination table name in the Bronze schema.
        engine (sqlalchemy.engine.Engine): The global Singleton database connection engine.
        select_columns (list, optional): A list of specific column names to load. 
                                         If None, all columns are loaded. Defaults to None.

    Raises:
        Exception: If the truncation or data loading fails, logs the error and raises the exception.
    """
    try:
        logging.info(f"Truncating table: bronze.{table_name}")

        # Open a connection to execute raw SQL for table truncation (Idempotency)
        with engine.connect() as conn:
            truncate_query = text(f"TRUNCATE TABLE bronze.{table_name}")
            conn.execute(truncate_query)
            conn.commit()
        logging.info(f"Table bronze.{table_name} truncated successfully.")

        logging.info(f"Loading data from {file_name} into bronze.{table_name}...")
        
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(file_name)

        # Filter the DataFrame to include only the specified columns, if provided
        if select_columns:
            df = df[select_columns]

        # ---------------------------------------------------------------------
        # Dynamic Column Renaming for CSVs
        # ---------------------------------------------------------------------
        # Define a mapping dictionary to align raw CSV column names with the standardized SQL schema.
        TABLE_MAPPING = {
            'taxi_location_raw': {
                'LocationID': 'location_id',
                'Borough': 'borough',
                'Zone': 'zone_name'
            },
            'weather_raw': {
                'datetime': 'date_time'
            }
        }

        # Safely attempt to retrieve the renaming rules for the current table.
        current_mapping = TABLE_MAPPING.get(table_name)

        # If a mapping exists for this specific table, apply the renaming operation.
        if current_mapping:
            df = df.rename(columns=current_mapping)
            logging.info(f"Columns renamed for table bronze.{table_name} as per mapping: {current_mapping}")

        # Insert the transformed DataFrame into the SQL Server table
        df.to_sql(
            name=table_name,
            schema="bronze",
            con=engine,
            if_exists='append',
            index=False
        )
        logging.info(f"Successfully loaded {len(df)} records into bronze.{table_name}.")
    
    except Exception as e:
        logging.error(f"Failed to load CSV data into bronze.{table_name}. Error: {e}")
        raise


def load_parquet_to_bronze(file_name, table_name, engine, select_columns=None):
    """
    Loads data from a large Parquet file into the Bronze schema using a chunked ingestion method.
    It truncates the existing table prior to insertion to maintain idempotency, and 
    renames columns within each batch to match the target database schema.

    Args:
        file_name (str): The absolute path to the source Parquet file.
        table_name (str): The destination table name in the Bronze schema.
        engine (sqlalchemy.engine.Engine): The global Singleton database connection engine.
        select_columns (list, optional): A list of specific column names to load. 
                                         If None, all columns are loaded. Defaults to None.

    Raises:
        Exception: If the truncation or chunked data loading fails, logs the error and raises the exception.
    """
    try:
        logging.info(f"Truncating table: bronze.{table_name}")

        # Open a connection to execute raw SQL for table truncation (Idempotency)
        with engine.connect() as conn:
            truncate_query = text(f"TRUNCATE TABLE bronze.{table_name}")
            conn.execute(truncate_query)
            conn.commit()
        logging.info(f"Table bronze.{table_name} truncated successfully.")

        logging.info(f"Starting chunked ingestion from {file_name} into bronze.{table_name}...")

        # Open the Parquet file using pyarrow to allow memory-efficient reading
        parquet_file = pq.ParquetFile(file_name)
        batch_counter = 1

        # Iterate over the Parquet file in batches (chunks) of 100,000 rows
        for batch in parquet_file.iter_batches(batch_size=100000, columns=select_columns):
            
            # Convert the pyarrow RecordBatch into a Pandas DataFrame
            df_chunk = batch.to_pandas()

            # ---------------------------------------------------------------------
            # Inline Column Renaming for Parquet Chunks
            # ---------------------------------------------------------------------
            # Standardize column names for the current chunk before attempting database insertion.
            df_chunk = df_chunk.rename(columns={
                'VendorID': 'vendor_id', 
                'tpep_pickup_datetime': 'pickup_datetime', 
                'tpep_dropoff_datetime': 'dropoff_datetime', 
                'PULocationID': 'pulocation_id', 
                'DOLocationID': 'dolocation_id'
            })
            
            # Insert the current DataFrame chunk into the SQL Server table
            df_chunk.to_sql(
                name=table_name,
                schema="bronze",
                con=engine,
                if_exists='append',
                index=False
            )
            logging.info(f"Batch {batch_counter} loaded successfully into bronze.{table_name}.")
            batch_counter += 1

        logging.info(f"Successfully completed chunked ingestion into bronze.{table_name}.")
    
    except Exception as e:
        logging.error(f"Failed to load Parquet data into bronze.{table_name}. Error: {e}")
        raise


# ==============================================================================
# 3. Pipeline Orchestration
# ==============================================================================

def orchestrator_bronze():
    """
    The Orchestrator Bronze function. It triggers the ingestion functions for 
    all required CSV and Parquet datasets into the Bronze layer, utilizing 
    the globally imported database engine.
    """

    # 3.1. Ingest dimension tables (CSV files)
    load_csv_to_bronze(
        file_name=date_path,
        table_name="date_raw",
        engine=engine
    )

    load_csv_to_bronze(
        file_name=location_path,
        table_name="taxi_location_raw",
        engine=engine
    )

    load_csv_to_bronze(
        file_name=weather_path,
        table_name="weather_raw",
        engine=engine,
        # Select specific columns to load, excluding unwanted columns from the raw weather API data
        select_columns=["datetime", "day_datetime", "temp", 
                        "feelslike", "preciptype", "precip", 
                        "snow", "snowdepth", "windspeed", 
                        "windgust", "visibility", "conditions", "icon"
                        ] 
    )

    # 3.2. Ingest the main fact table (Parquet file) using chunking
    load_parquet_to_bronze(
        file_name=parquet_path,
        table_name="taxi_trips_raw",
        engine=engine,
        # Select specific columns relevant to the data warehouse schema
        select_columns=["VendorID", "tpep_pickup_datetime", 
                        "tpep_dropoff_datetime", "passenger_count", 
                        "trip_distance", "PULocationID", "DOLocationID", 
                        "fare_amount", "tip_amount", "total_amount"
                        ]
    )
    
    logging.info("=== ETL process for Bronze layer completed successfully ===")


# ==============================================================================
# 4. Script Entry Point
# ==============================================================================

# Entry point of the script
if __name__ == "__main__":
    orchestrator_bronze()

    # Provide immediate visual feedback in the console upon successful execution,
    # directing the user to the dedicated log file for detailed execution history.
    print("ETL process for Bronze layer completed successfully. Check the log file for details.")