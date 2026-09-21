"""
Centralized Configuration and Path Management
-------------------------------------------
This module acts as the centralized configuration hub for the NYC Taxi Data Warehouse ETL pipeline.
It utilizes the 'pathlib' library to dynamically resolve and construct absolute paths for the 
project directory, raw data files, and log files. 

By centralizing these paths, it ensures the DRY (Don't Repeat Yourself) principle across the project.
It also proactively creates necessary directories (e.g., 'data' and 'logs') to prevent 
FileNotFoundError exceptions during runtime.
"""

# Importing Path from pathlib for object-oriented filesystem path manipulation. 
# This approach is more robust, readable, and cross-platform compatible compared to traditional os.path strings.
from pathlib import Path

# ==========================================
# 1. Project Root Directory Resolution
# ==========================================
# Resolve the absolute path to the project root directory.
# Note: '.parent.parent.parent' navigates three levels up from the current file's directory.
# Ensure this matches your actual project directory structure.
project_path = Path(__file__).resolve().parent.parent.parent

# ==========================================
# 2. Data Directory and File Paths
# ==========================================
# Define the main 'data' directory path where raw files are stored
data_path = project_path.joinpath(r"data")

# Create the 'data' directory automatically if it does not already exist
data_path.mkdir(parents=True, exist_ok=True)

# Define paths for the various source data files expected in the 'data' directory
weather_path = data_path.joinpath(r"weather_data.csv")
date_path = data_path.joinpath(r"dim_date.csv")
parquet_path = data_path.joinpath(r"yellow_tripdata_2026-01.parquet")
location_path = data_path.joinpath(r"taxi_zone_lookup.csv")

# ==========================================
# 3. Logs Directory and File Paths
# ==========================================
# Define the 'logs' directory path where pipeline execution logs will be saved
logs_path = project_path.joinpath(r"logs")

# Create the 'logs' directory automatically if it does not already exist
logs_path.mkdir(parents=True, exist_ok=True)

# Define specific log file paths for each layer of the Medallion Architecture (Bronze, Silver, Gold)
bronze_log_path = logs_path.joinpath(r"etl_bronze.log")
silver_log_path = logs_path.joinpath(r"etl_silver.log")
gold_log_path = logs_path.joinpath(r"etl_gold.log")