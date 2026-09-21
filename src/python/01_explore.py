"""
Data Exploration and Profiling Script
-------------------------------------
This script is designed to explore large Parquet files (specifically the NYC Taxi dataset 
for January 2026) efficiently without overloading the system's memory (RAM). 

It dynamically resolves file paths using a centralized configuration, safely verifies 
file existence, extracts file metadata (rows, row groups, schema), and loads a highly 
restricted sample (10 rows) to perform initial data profiling. This profiling includes 
reviewing data types, identifying missing values (nulls), and generating basic descriptive 
statistics to prepare for the ETL pipeline's subsequent Silver and Gold layers.
"""

# Importing Path from pathlib for object-oriented filesystem path manipulation.
from pathlib import Path

# Importing pandas for data manipulation, DataFrame creation, and profiling functions.
import pandas as pd

# Importing pyarrow.parquet to handle Parquet files efficiently without loading them entirely into memory.
import pyarrow.parquet as pq

# Importing the absolute parquet file path from the centralized custom configuration module.
from config import parquet_path

# =====================================================================
# 1. Pandas Display Configuration
# =====================================================================
# Configure pandas to display all columns in the console without truncating them (useful for wide datasets).
pd.set_option('display.max_columns', None)

# Configure pandas to display the full content of each column without truncating long strings.
pd.set_option('display.max_colwidth', None)

# Format all floating-point numbers in pandas console outputs to display with exactly 2 decimal places.
pd.set_option('display.float_format', '{:.2f}'.format)

# =====================================================================
# 2. Dynamic Path Resolution & Fail-Safe Check
# =====================================================================
# Print a clear visual header for the file path output.
print("--- Parquet File Path ---")

# Output the resolved absolute path to verify it points to the correct location.
print(parquet_path)

# Fail-Safe: Verify if the file actually exists before attempting to read it.
# This prevents the script from crashing with a FileNotFoundError if the file is missing.
if parquet_path.exists():
    
    # =====================================================================
    # 3. Metadata Extraction (Memory Efficient)
    # =====================================================================
    # Open a lightweight pointer to the Parquet file. 
    # This reads only the metadata footer of the file, completely avoiding loading the dataset into RAM.
    parquet_file = pq.ParquetFile(parquet_path)

    # Print a header and output the total number of rows stored in the Parquet file metadata.
    print("\n--- Number of Rows ---")
    print(parquet_file.metadata.num_rows)

    # Print a header and output the total number of row groups (chunks of data) in the Parquet file.
    print("\n--- DataFrame Row Groups ---")
    print(parquet_file.metadata.num_row_groups)

    # Print a header and output the exact schema names (column names) as defined in the Parquet file.
    print("\n--- DataFrame Schema ---")
    print(parquet_file.metadata.schema.names)

    # =====================================================================
    # 4. Data Sampling using Generators
    # =====================================================================
    # Create a generator object that streams the Parquet data in very small batches (10 rows per batch).
    # This is crucial for handling large files safely.
    generator = parquet_file.iter_batches(batch_size=10)

    # Use the next() function to fetch strictly the first batch from the generator, keeping memory footprint minimal.
    first_batch = next(generator)

    # Convert the extracted PyArrow RecordBatch into a standard pandas DataFrame for easier viewing and analysis.
    df_sample = first_batch.to_pandas()

    # Print a header and output the first 10 rows of the dataset.
    print("\n--- DataFrame Sample ---")
    print(df_sample)

    # =====================================================================
    # 5. Data Profiling
    # =====================================================================
    # Inspect the DataFrame structure, including column names, non-null counts, and data types.
    print("\n--- DataFrame Info ---")
    # Call info() directly without wrapping it in a print() statement to prevent printing 'None' to the console.
    df_sample.info()

    # Calculate and output the exact count of missing (Null/NaN) values for every single column.
    print("\n--- Missing Values ---")
    print(df_sample.isnull().sum())

    # Generate and output descriptive statistics (mean, min, max, percentiles) for all numeric columns.
    # This helps in quickly identifying anomalies like negative fares or zero passenger counts.
    print("\n--- Basic Statistics ---")
    print(df_sample.describe())

else:
    # Output a clear error message if the file is missing, allowing a graceful exit instead of a system crash.
    print(f"\n[Error] The file was not found at the specified path: {parquet_path}")
    print("Please ensure the Parquet file is downloaded and correctly placed in the 'data' directory.")