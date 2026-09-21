"""
Weather API Data Extraction Script
----------------------------------
This script extracts hourly weather data for New York City for January 2026
using the Visual Crossing Weather API. It flattens the nested JSON response
into a pandas DataFrame and saves it as a CSV file in the project's data directory.
It includes a retry mechanism to handle API rate limits (HTTP 429) and an 
idempotency check to protect the API quota by skipping the request if data already exists.
"""

# Importing requests to send HTTP GET requests and communicate with the Visual Crossing Weather API.
import requests as req

# Importing time to pause execution (sleep) when handling API rate limits (HTTP 429) during retry attempts.
import time

# Importing pandas to flatten the nested JSON response and export the tabular data to a CSV file.
import pandas as pd

# Importing os to interact with the operating system, specifically to retrieve sensitive environment variables.
import os

# Importing load_dotenv to securely load sensitive credentials (like the API key) from a local .env file.
from dotenv import load_dotenv

# Importing the dynamically resolved absolute file path for the weather data from the centralized configuration module.
from config import weather_path

# =====================================================================
# 1. Environment and API Configuration
# =====================================================================
# Load environment variables from the hidden .env file into the system's memory.
# This ensures sensitive data is accessible without being hardcoded in the script.
load_dotenv()

# Retrieve the securely stored API key from the environment variables.
api_key = os.getenv("API_KEY")

# Define the dynamic API endpoint URL using an f-string.
# It requests metric units and hourly data inclusion, appending the loaded API key securely.
api_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/New%20York/2026-01-01/2026-01-31?unitGroup=metric&include=hours&key={api_key}"
        
# =====================================================================
# 2. Idempotency Check & API Extraction
# =====================================================================
# Idempotency Check: Verify if the data file already exists before hitting the API.
# Because the extraction logic is placed inside the 'else' block, finding the file successfully bypasses the API call, protecting the quota.
if weather_path.exists():
    print(f"Data already exists at {weather_path}.")
    print("Skipping API request to protect your daily quota.")
else:
    print("File not found. Initiating API request...")

    # Implement a retry mechanism to handle potential connection issues or rate limits.
    # The loop will attempt the API request up to 3 times before failing completely.
    for attempt in range(3): 
        try:
            # Send the GET request to the API endpoint with a 5-second timeout threshold to prevent indefinite hanging.
            response = req.get(api_url, timeout=5)
            
            # Check the HTTP status code. If it indicates an error (e.g., 404, 500), it raises an HTTPError.
            # This applies the Fail-Fast engineering principle.
            response.raise_for_status()

            print("API request successful.")
            
            # Parse the incoming JSON response payload into a Python dictionary.
            data = response.json()

            # =====================================================================
            # 3. Data Transformation and Export
            # =====================================================================
            # Flatten the nested JSON structure to extract hourly data into tabular rows.
            # 'record_path' targets the list of hours inside each day.
            # 'meta' retains the parent day's date, prefixed with 'day_' to avoid column name collisions.
            weather_df = pd.json_normalize(
                data['days'],
                record_path='hours',
                meta=['datetime'],
                meta_prefix='day_'
            )
            
            # Ensure the target directory exists before attempting to save the file.
            # parents=True creates intermediate directories if needed; exist_ok=True ignores the command if the folder exists.
            weather_path.parent.mkdir(parents=True, exist_ok=True) 

            # Export the pandas DataFrame to a CSV file.
            # index=False ensures the DataFrame index is not written as a separate column in the file.
            weather_df.to_csv(weather_path, index=False)
            print("Weather data saved to weather_data.csv")
            
            # Exit the retry loop entirely since the extraction and saving process completed successfully.
            break
        
        except req.exceptions.HTTPError as err:
            # Handle rate limiting (HTTP 429 Too Many Requests) specifically by pausing execution.
            if response.status_code == 429:
                print("Too many requests. Retrying after 60 seconds...")
                # Sleep for 60 seconds to allow the API quota/rate limit window to reset.
                time.sleep(60)
            # Raise any other critical HTTP errors immediately to stop execution (Fail-Fast).
            else:
                print(f"Critical API Error: {err}")
                raise