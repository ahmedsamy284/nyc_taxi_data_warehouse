"""
Common Utilities and Connection Management
------------------------------------------
This module centralizes shared utility functions used across all ETL layers 
(Bronze, Silver, Gold) of the NYC Taxi Data Warehouse pipeline.

It serves two primary purposes:
1. Dynamic Logging Setup: Provides a unified method to configure logging paths per layer.
2. Singleton Database Engine: Instantiates a single, globally accessible SQLAlchemy engine
   with optimized bulk insert settings (fast_executemany) to enforce the DRY principle.
"""

# Importing os to interact with the operating system, specifically to retrieve environment variables.
import os

# Importing load_dotenv to securely load sensitive credentials (e.g., DB server, driver) from a local .env file.
from dotenv import load_dotenv

# Importing create_engine from sqlalchemy to build a robust database connection object.
from sqlalchemy import create_engine

# Importing urllib to safely parse and encode URL-unsafe characters in the ODBC connection string.
import urllib

# Importing logging to track pipeline execution events, monitor progress, and record errors.
import logging

# =====================================================================
# 1. Environment Initialization
# =====================================================================
# Load environment variables from the hidden .env file into the system's memory immediately upon module import.
load_dotenv()

# =====================================================================
# 2. Shared Logging Configuration
# =====================================================================
def setup_logging(log_file_path):
    """
    Configures the basic logging settings dynamically for the calling ETL script.
    
    Args:
        log_file_path (pathlib.Path): The absolute path where the log file should be written.
    """
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        filemode='a',
        format='%(asctime)s - %(levelname)s - %(message)s',
        force=True  # Forces the reconfiguration of the root logger to point to the new file per layer
    )

# =====================================================================
# 3. Database Connection Parameters
# =====================================================================
# Retrieve database connection parameters securely from the loaded environment variables.
driver_name = os.getenv("DRIVER")
server_name = os.getenv("DB_SERVER")
database_name = os.getenv("DB_NAME")

# =====================================================================
# 4. Engine Creation Logic
# =====================================================================
def get_db_engine(driver_name, server_name, database_name):
    """
    Establishes a connection to the SQL Server database using SQLAlchemy and pyodbc.
    It is specifically configured to optimize bulk data insertions.

    Args:
        driver_name (str): The ODBC driver name (e.g., 'ODBC Driver 17 for SQL Server').
        server_name (str): The SQL Server instance name.
        database_name (str): The target database name.

    Returns:
        sqlalchemy.engine.Engine: A configured SQLAlchemy engine for database operations.

    Raises:
        Exception: If the database connection fails, logs the error and raises the exception.
    """
    try:
        # Note: We use print here initially in case setup_logging hasn't been called yet by the importing script.
        logging.info("Attempting to establish global database engine connection...")
        
        # Construct the raw ODBC connection string using Windows Authentication (Trusted_Connection)
        connection_string = (
            f"DRIVER={driver_name};"
            f"SERVER={server_name};"
            f"DATABASE={database_name};"
            f"Trusted_Connection=yes;"
        )

        # Parse the connection string to be URL-safe for SQLAlchemy
        params = urllib.parse.quote_plus(connection_string)
        
        # Create the SQLAlchemy engine leveraging default pooling mechanisms.
        # fast_executemany=True drastically reduces I/O overhead by sending data in bulk batches.
        engine = create_engine(
            f"mssql+pyodbc:///?odbc_connect={params}", 
            fast_executemany=True      
        )  

        logging.info("Global database engine established successfully.")
        return engine
    
    except Exception as e:
        logging.error(f"CRITICAL ERROR: Global database connection failed: {e}")
        raise
    
# =====================================================================
# 5. Singleton Engine Instantiation
# =====================================================================
# Instantiate the database engine once upon module import.
# Other scripts will simply import this 'engine' variable, preventing redundant connections.
engine = get_db_engine(driver_name, server_name, database_name)