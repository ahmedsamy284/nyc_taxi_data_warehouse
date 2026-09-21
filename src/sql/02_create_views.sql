/*
===================================================================================================
Project:        NYC Taxi Data Warehouse - Medallion Architecture
Layer:          Gold (Semantic Layer / Views)
Description:    This script creates the analytical views on top of the Gold Star Schema. 
                These views abstract the underlying complexity (joins, surrogate keys) and 
                provide ready-to-use, business-friendly, and denormalized datasets for BI 
                tools (e.g., Power BI) and Data Analysts.
Execution:      Run this script entirely in SQL Server Management Studio (SSMS).
===================================================================================================
*/

USE NYC_Taxi_DW;
GO

/*
===================================================================================================
1. Master Trip Analysis View (vw_trip_analysis_master)
---------------------------------------------------------------------------------------------------
Purpose: Provides a comprehensive, row-level denormalized view of all taxi trips. 
         It joins the central fact table with all associated dimension tables (Date, 
         Location, Weather) to translate surrogate keys into readable business attributes.
Usage:   General reporting, detailed dashboarding, row-level auditing, and ad-hoc analysis.
===================================================================================================
*/

-- Drop the view if it already exists to ensure idempotency and allow easy updates
DROP VIEW IF EXISTS gold.vw_trip_analysis_master;
GO

CREATE VIEW gold.vw_trip_analysis_master AS
SELECT 
    -- ==========================================
    -- 1. Fact Table Measures & Identifiers
    -- ==========================================
    f.trip_key,                  -- Unique identifier for the trip (useful for auditing and BI drill-throughs)
    f.vendor_id,                 -- ID of the taxi vendor
    f.passenger_count,           -- Number of passengers in the vehicle
    f.trip_distance_miles,       -- Distance of the trip in miles
    f.fare_amount_usd,           -- Base fare cost
    f.tip_amount_usd,            -- Tip amount given by the passenger
    f.total_amount_usd,          -- Total cost of the trip
    f.pickup_hour,               -- Degenerate dimension representing the hour of the day the trip started
    
    -- ==========================================
    -- 2. Pickup Date Information
    -- ==========================================
    pickup_dt.full_date AS pickup_date,                 -- Actual calendar date of the pickup
    pickup_dt.day_name AS pickup_day_name,              -- Name of the day (e.g., Monday, Tuesday)
    pickup_dt.is_weekend_flag AS is_pickup_weekend,     -- Boolean flag indicating if the pickup day is a weekend
    
    -- ==========================================
    -- 3. Dropoff Date Information
    -- ==========================================
    dropoff_dt.full_date AS dropoff_date,               -- Actual calendar date of the dropoff
    dropoff_dt.day_name AS dropoff_day_name,            -- Name of the day for the dropoff
    
    -- ==========================================
    -- 4. Pickup Location Information
    -- ==========================================
    pickup_loc.borough_name AS pickup_borough,          -- Name of the NYC borough where the trip started
    pickup_loc.taxi_zone_name AS pickup_zone,           -- Specific taxi zone of the pickup
    
    -- ==========================================
    -- 5. Dropoff Location Information
    -- ==========================================
    dropoff_loc.borough_name AS dropoff_borough,        -- Name of the NYC borough where the trip ended
    dropoff_loc.taxi_zone_name AS dropoff_zone,         -- Specific taxi zone of the dropoff
    
    -- ==========================================
    -- 6. Weather Information
    -- ==========================================
    w.weather_condition_desc AS weather_condition,      -- Text description of the weather (e.g., Clear, Rain)
    w.temperature_celsius AS temp_celsius               -- Temperature in Celsius at the time of the trip

FROM gold.fact_taxi_trips AS f

-- LEFT JOINs are used to ensure no fact rows are dropped even if dimension data is missing

-- JOIN 1: Resolve pickup date surrogate key
LEFT JOIN gold.dim_date AS pickup_dt
    ON f.pickup_date_key = pickup_dt.date_key
    
-- JOIN 2: Resolve dropoff date surrogate key (using dropoff_dt alias to distinguish from pickup)
LEFT JOIN gold.dim_date AS dropoff_dt
    ON f.dropoff_date_key = dropoff_dt.date_key

-- JOIN 3: Resolve pickup location surrogate key
LEFT JOIN gold.dim_location AS pickup_loc
    ON f.pickup_location_key = pickup_loc.location_key
    
-- JOIN 4: Resolve dropoff location surrogate key (using dropoff_loc alias to distinguish from pickup)
LEFT JOIN gold.dim_location AS dropoff_loc
    ON f.dropoff_location_key = dropoff_loc.location_key
    
-- JOIN 5: Resolve weather surrogate key
LEFT JOIN gold.dim_weather AS w
    ON f.weather_key = w.weather_key;

GO


/*
===================================================================================================
2. Weather Impact Summary View (vw_weather_impact_summary)
---------------------------------------------------------------------------------------------------
Purpose: Aggregates core taxi trip metrics (total count, total revenue, average tip, 
         average distance) grouped by weather conditions.
Usage:   Trend analysis to understand how different weather conditions affect taxi 
         operations, rider behavior, and revenue generation.
===================================================================================================
*/

-- Drop the view if it already exists for idempotency
DROP VIEW IF EXISTS gold.vw_weather_impact_summary;
GO

CREATE VIEW gold.vw_weather_impact_summary AS
SELECT 
    -- 1. Grouping Attribute: Weather Dimension
    w.weather_condition_desc AS weather_condition,
    
    -- 2. Aggregated Metrics
    COUNT(f.trip_key) AS total_trips,             -- Total number of trips under this weather condition
    SUM(f.total_amount_usd) AS total_revenue,     -- Total revenue generated
    AVG(f.tip_amount_usd) AS avg_tip,             -- Average tip amount given by passengers
    AVG(f.trip_distance_miles) AS avg_distance    -- Average distance traveled per trip

FROM gold.fact_taxi_trips AS f

-- LEFT JOIN to preserve trips even if weather data is unavailable for that specific time
LEFT JOIN gold.dim_weather AS w
    ON f.weather_key = w.weather_key

-- Group the aggregated results by the weather condition description
GROUP BY w.weather_condition_desc;

GO


/*
===================================================================================================
3. Location Performance View (vw_location_performance)
---------------------------------------------------------------------------------------------------
Purpose: Aggregates high-level taxi trip metrics grouped by the pickup borough.
Usage:   Spatial analysis and location-based revenue tracking to identify the most 
         profitable and high-demand geographical areas.
===================================================================================================
*/

-- Drop the view if it already exists for idempotency
DROP VIEW IF EXISTS gold.vw_location_performance;
GO

CREATE VIEW gold.vw_location_performance AS
SELECT 
    -- 1. Grouping Attribute: Location Dimension (Borough)
    loc.borough_name AS pickup_borough,
    
    -- 2. Aggregated Metrics
    COUNT(f.trip_key) AS total_trips,             -- Total number of trips originating from this borough
    SUM(f.total_amount_usd) AS total_revenue      -- Total revenue generated from trips starting in this borough

FROM gold.fact_taxi_trips AS f

-- LEFT JOIN to map the pickup location key to the corresponding borough name
LEFT JOIN gold.dim_location AS loc
    ON f.pickup_location_key = loc.location_key

-- Group the aggregated results by the pickup borough name
GROUP BY loc.borough_name;

GO