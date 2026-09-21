"""
Date Dimension (dim_date) Generation Script
-------------------------------------------
This script procedurally generates a comprehensive Date Dimension table 
for January 2026 to be used in the Star Schema of the NYC Taxi ETL pipeline.

It utilizes pandas to generate a localized sequence of dates and extract numerical 
and textual features (such as day name, day of the week, and weekend flags). 
Furthermore, it integrates the 'holidays' library to accurately tag US public holidays. 
The final enriched dataset is exported as a CSV file to the landing zone, eliminating 
the need for external API dependencies.
"""

# Importing the holidays library to dynamically fetch and validate official public holidays for specific countries (US).
import holidays

# Importing pandas for efficient time-series data generation, manipulation, and datetime operations.
import pandas as pd

# Importing the dynamically resolved absolute file path for the date dimension data from the centralized configuration module.
from config import date_path

# =====================================================================
# 1. Date Range Generation
# =====================================================================
# Generate a continuous sequence of dates (DatetimeIndex) starting from Jan 1, 2026 to Jan 31, 2026.
# The parameter freq="D" strictly sets the generation interval to one Day.
date_range = pd.date_range(start="2026-01-01", end="2026-01-31", freq="D")

# =====================================================================
# 2. DataFrame Creation
# =====================================================================
# Convert the generated array of dates into a structured pandas DataFrame.
# The newly created column holding these dates is explicitly named "full_date".
date_df = pd.DataFrame(date_range, columns=["full_date"])

# =====================================================================
# 3. Feature Extraction (Day Names & Numbers)
# =====================================================================
# Extract the textual string name of the day (e.g., 'Monday', 'Tuesday') from the datetime object.
date_df['day_name'] = date_df['full_date'].dt.day_name()

# Extract the numerical day of the week. 
# dt.dayofweek returns 0-6 (where 0 is Monday). Adding + 1 aligns it with the ISO standard (1 = Monday, 7 = Sunday).
date_df['day_of_week'] = date_df['full_date'].dt.dayofweek + 1

# =====================================================================
# 4. Weekend Flagging
# =====================================================================
# Create a boolean column 'is_weekend'. 
# The .isin([6, 7]) method checks if the numerical 'day_of_week' is either 6 (Saturday) or 7 (Sunday).
date_df['is_weekend'] = date_df['day_of_week'].isin([6, 7])

# =====================================================================
# 5. Public Holidays Integration
# =====================================================================
# Initialize the US holidays calendar explicitly for the target year (2026) to optimize memory and lookup speed.
us_holidays = holidays.US(years=2026)

# Create a boolean column 'is_holiday' to flag public holidays.
# The .apply() method iterates over each row, using a lambda function to extract just the .date() component
# (ignoring the timestamp) and evaluates if that specific date exists in the initialized us_holidays object.
date_df['is_holiday'] = date_df['full_date'].apply(lambda x: x.date() in us_holidays)

# =====================================================================
# 6. Export to Landing Zone
# =====================================================================
# Ensure the target 'data' directory exists before attempting to save. 
# parents=True creates any missing parent directories; exist_ok=True prevents errors if the folder already exists.
date_path.parent.mkdir(parents=True, exist_ok=True)

# Export the fully engineered Date Dimension DataFrame to a CSV file.
# The index=False argument ensures that pandas row indices are omitted from the final output file.
date_df.to_csv(date_path, index=False)