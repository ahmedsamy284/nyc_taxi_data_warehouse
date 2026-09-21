/*
======================================================================================
Project Name : NYC Taxi Data Warehouse Pipeline
Architecture : 3-Tier Medallion Architecture (Bronze -> Silver -> Gold)
Environment  : SQL Server (T-SQL)
Description  : Physical database schema setup and dimensional modeling for NYC Taxi 
               trips analytics.
               - BRONZE : Landing zone for raw, untyped data directly from source files.
               - SILVER : Cleansed, strongly-typed data with enforced business rules.
               - GOLD   : Star schema optimized for BI reporting and aggregations.
======================================================================================
*/

USE master;
GO

-- ===================================================================================
-- 1. DATABASE SETUP
-- Description: Drop the existing database to ensure a clean state, then recreate it.
--              This includes safely rolling back active connections to prevent locks.
-- ===================================================================================

IF EXISTS (SELECT 1 FROM sys.databases WHERE name ='NYC_Taxi_DW')
BEGIN
    -- Force disconnect all active connections before dropping to prevent lock errors
	ALTER DATABASE NYC_Taxi_DW SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE NYC_Taxi_DW;
END;
GO

-- Create the new database for the data warehouse
CREATE DATABASE NYC_Taxi_DW;
GO

USE NYC_Taxi_DW;
GO


-- ===================================================================================
-- 2. SCHEMA CREATION
-- Description: Organizing tables logically into Medallion architectural layers (Schemas).
-- ===================================================================================

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


-- ===================================================================================
-- 3. BRONZE LAYER: Raw Data Ingestion
-- Description: Stores data exactly as received from the source files. Constraints are 
--              removed and types are flexible to prevent bulk ingestion failures.
-- ===================================================================================

IF OBJECT_ID ('bronze.taxi_location_raw','U') IS NOT NULL
	DROP TABLE bronze.taxi_location_raw;
GO

CREATE TABLE bronze.taxi_location_raw (
	location_id          INT,           -- Original TLC Location ID from source
	borough              VARCHAR(100),  -- NYC Borough name (e.g., Manhattan, Queens)
	zone_name            VARCHAR(255),  -- Specific taxi zone or neighborhood name
	service_zone         VARCHAR(100)   -- Type of service zone (e.g., Boro Zone, Yellow Zone)
);
GO



IF OBJECT_ID ('bronze.date_raw','U') IS NOT NULL
	DROP TABLE bronze.date_raw;
GO

CREATE TABLE bronze.date_raw (
	full_date                DATE,          -- The calendar date
	day_name                 VARCHAR(50),   -- String representation of the day (e.g., Monday)
	day_of_week              INT,           -- Numeric day of the week (1 to 7)
	is_weekend               VARCHAR(10),   -- Kept as string to safely catch 'True'/'False' from Pandas
    is_holiday               VARCHAR(10)    -- Kept as string to safely catch 'True'/'False' from Pandas
);
GO



IF OBJECT_ID ('bronze.weather_raw','U') IS NOT NULL
	DROP TABLE bronze.weather_raw;
GO

CREATE TABLE bronze.weather_raw (
	date_time          VARCHAR(50),   -- Raw time string from source (e.g., 18:00:00)
	day_datetime       DATE,          -- Truncated date portion for joining purposes
	temp               FLOAT,         -- Actual temperature recorded
	feelslike          FLOAT,         -- Feels-like temperature
	preciptype         VARCHAR(100),  -- Type of precipitation (Rain/Snow)
    precip             FLOAT,         -- Amount of precipitation
	snow               FLOAT,         -- Amount of snowfall
	snowdepth          FLOAT,         -- Accumulated snow depth
	windspeed          FLOAT,         -- Measured wind speed
	windgust           FLOAT,         -- Measured wind gust speed
	visibility         FLOAT,         -- Visibility distance 
	conditions         VARCHAR(255),  -- Textual description of weather conditions (e.g., Overcast)
	icon               VARCHAR(100)   -- Short condition code for UI/Icons rendering
);
GO



IF OBJECT_ID ('bronze.taxi_trips_raw','U') IS NOT NULL
	DROP TABLE bronze.taxi_trips_raw;
GO

CREATE TABLE bronze.taxi_trips_raw (
	vendor_id             INT,         -- TPEP provider code (e.g., 1=Creative Mobile, 2=VeriFone)
	pickup_datetime       DATETIME,    -- Exact date and time the meter was engaged
	dropoff_datetime      DATETIME,    -- Exact date and time the meter was disengaged
	passenger_count       FLOAT,       -- Float used to handle Pandas NaN values safely during insert
    trip_distance         FLOAT,       -- Elapsed trip distance in miles
	pulocation_id         INT,         -- TLC Taxi Zone ID where the passenger was picked up
	dolocation_id         INT,         -- TLC Taxi Zone ID where the passenger was dropped off
	fare_amount           FLOAT,       -- Time-and-distance fare calculated by the meter
	tip_amount            FLOAT,       -- Gratuity amount given to the driver
	total_amount          FLOAT        -- Total amount charged to passengers (Includes tips and taxes)
);
GO

-- ===================================================================================
-- 4. SILVER LAYER: Cleansed and Standardized Data
-- Description: Data is cast to target data types. Essential business constraints 
--              (like NOT NULL and Primary Keys) are enforced.
-- ===================================================================================

IF OBJECT_ID('silver.taxi_location','U') IS NOT NULL
    DROP TABLE silver.taxi_location;
GO

CREATE TABLE silver.taxi_location (
	location_id          INT PRIMARY KEY,  -- Enforced PK as it's a unique natural key
	borough              VARCHAR(100),     -- Cleaned NYC Borough name
	zone_name            VARCHAR(255),     -- Cleaned Taxi Zone name
	service_zone         VARCHAR(100)      -- Cleaned Service Zone type
);
GO


IF OBJECT_ID('silver.dim_date','U') IS NOT NULL
    DROP TABLE silver.dim_date;
GO

CREATE TABLE silver.dim_date (
	full_date                DATE PRIMARY KEY,  -- Enforced PK for unique calendar dates
	day_name                 VARCHAR(50),       -- Validated day name
	day_of_week              INT,               -- Validated numeric day
	is_weekend               BIT,               -- Converted from text to standard boolean (1/0)
    is_holiday               BIT                -- Converted from text to standard boolean (1/0)
);
GO



IF OBJECT_ID('silver.weather','U') IS NOT NULL
    DROP TABLE silver.weather;
GO

CREATE TABLE silver.weather (
	full_datetime	   DATETIME NOT NULL,       -- Unified timestamp key for temporal joins
	date_time          VARCHAR(50) NOT NULL,    -- Time of the weather recording
	day_datetime       DATE NOT NULL,           -- Enforced NOT NULL for vital date linkage
	temp               FLOAT,				    -- Cleansed temperature data
	feelslike          FLOAT,				   	-- Cleansed feels-like data
	preciptype         VARCHAR(100),		    -- Validated precipitation type
    precip             FLOAT,					-- Validated precipitation amount
	snow               FLOAT,				    -- Validated snowfall amount
	snowdepth          FLOAT,					-- Validated snow depth
	windspeed          FLOAT,					-- Validated wind speed
	windgust           FLOAT,					-- Validated wind gust
	visibility         FLOAT,					-- Validated visibility
	conditions         VARCHAR(255),			-- Validated weather conditions
	icon               VARCHAR(100)				-- Validated UI icon string
);
GO




IF OBJECT_ID('silver.taxi_trips','U') IS NOT NULL
    DROP TABLE silver.taxi_trips;
GO

CREATE TABLE silver.taxi_trips (
	vendor_id             INT,               -- Validated provider code
	pickup_datetime       DATETIME NOT NULL, -- Enforced NOT NULL for critical business logic
	dropoff_datetime      DATETIME NOT NULL, -- Enforced NOT NULL for critical business logic
	passenger_count       INT,               -- Cleaned and cast back to Integer (imputing NaNs)
    trip_distance         FLOAT,             -- Validated trip distance
	pulocation_id         INT,               -- Standardized column name for Pickup Location
	dolocation_id         INT,               -- Standardized column name for Dropoff Location
	fare_amount           FLOAT,             -- Validated fare amount
	tip_amount            FLOAT,             -- Validated tip amount
	total_amount          FLOAT              -- Validated total amount
);
GO



-- ===================================================================================
-- 5. GOLD LAYER: Dimensional Modeling (Star Schema)
-- Description: Optimized for fast analytical queries. Contains Dimension tables 
--              (descriptive attributes) and a Fact table (measurable metrics). 
--              Surrogate keys are generated using IDENTITY(1,1).
-- ===================================================================================

/*
--------------------------------------------------------------------------------------
Table: gold.dim_date
Type : Dimension
Grain: One row per calendar date.
--------------------------------------------------------------------------------------
*/
IF OBJECT_ID('gold.dim_date','U') IS NOT NULL
    DROP TABLE gold.dim_date;
GO

CREATE TABLE gold.dim_date (
    date_key                 INT PRIMARY KEY IDENTITY(1,1), -- Surrogate Key
	full_date                DATE,                          -- Natural Key
	day_name                 VARCHAR(50),                   -- Descriptive day name
	day_of_week_num          INT,                           -- Appended _num for clarity
	is_weekend_flag          BIT,                           -- Appended _flag for clarity
    is_holiday_flag          BIT                            -- Appended _flag for clarity
);
GO


/*
--------------------------------------------------------------------------------------
Table: gold.dim_location
Type : Dimension
Grain: One row per unique TLC Taxi Zone.
--------------------------------------------------------------------------------------
*/
IF OBJECT_ID('gold.dim_location','U') IS NOT NULL
    DROP TABLE gold.dim_location;
GO

CREATE TABLE gold.dim_location (
	location_key         INT PRIMARY KEY IDENTITY(1,1), -- Surrogate Key for Data Warehouse linkage
 	location_id          INT,                           -- Natural Key from source system
	borough_name         VARCHAR(100),                  -- Renamed for business clarity
	taxi_zone_name       VARCHAR(255),                  -- Renamed for business clarity
	service_zone         VARCHAR(100)                   -- Renamed for business clarity
);
GO


/*
--------------------------------------------------------------------------------------
Table: gold.dim_weather
Type : Dimension
Grain: One row per hourly weather reading.
--------------------------------------------------------------------------------------
*/
IF OBJECT_ID('gold.dim_weather','U') IS NOT NULL
    DROP TABLE gold.dim_weather;
GO

CREATE TABLE gold.dim_weather (
    weather_key              INT PRIMARY KEY IDENTITY(1,1), -- Surrogate Key
	weather_date             DATETIME,                      -- Date & time of the weather reading
	temperature_celsius      FLOAT,                         -- Explicit metric naming 
	precipitation_type       VARCHAR(100),                  -- Descriptive type (Rain/Snow)
	snow_depth_cm            FLOAT,                         -- Explicit metric naming 
	wind_speed_kmh           FLOAT,                         -- Explicit metric naming 
	weather_condition_desc   VARCHAR(255)                   -- Descriptive name
);
GO


/*
--------------------------------------------------------------------------------------
Table: gold.fact_taxi_trips
Type : Transactional Fact Table
Grain: One row per individual completed taxi trip.
--------------------------------------------------------------------------------------
*/
IF OBJECT_ID('gold.fact_taxi_trips','U') IS NOT NULL
    DROP TABLE gold.fact_taxi_trips;
GO

CREATE TABLE gold.fact_taxi_trips (

    -- ==========================================
    -- Primary Keys 
    -- ==========================================
	trip_key                 INT PRIMARY KEY IDENTITY(1,1),

-- ==============================================================================
    -- Foreign Key Definitions (Surrogate Keys)
    -- Explicitly named constraints are used to ensure predictable schema management, 
    -- allowing safe DROP and ADD operations during ETL truncation processes.
    -- ==============================================================================
    
    pickup_date_key          INT,
    dropoff_date_key         INT,
    pickup_location_key      INT,
    dropoff_location_key     INT,
    weather_key              INT,

    -- Constraint Definitions
    CONSTRAINT fk_fact_pickup_date      FOREIGN KEY (pickup_date_key)      REFERENCES gold.dim_date(date_key),
    CONSTRAINT fk_fact_dropoff_date     FOREIGN KEY (dropoff_date_key)     REFERENCES gold.dim_date(date_key),
    CONSTRAINT fk_fact_pickup_location  FOREIGN KEY (pickup_location_key)  REFERENCES gold.dim_location(location_key),
    CONSTRAINT fk_fact_dropoff_location FOREIGN KEY (dropoff_location_key) REFERENCES gold.dim_location(location_key),
    CONSTRAINT fk_fact_weather          FOREIGN KEY (weather_key)          REFERENCES gold.dim_weather(weather_key),
	
	-- ==========================================
    -- Degenerate Dimensions & Time Elements
    -- ==========================================
	vendor_id                INT,     -- Provider code (1=Creative Mobile, 2=VeriFone)
	pickup_hour              INT,     -- Hour of the day the trip started (0-23)

    -- ==========================================
    -- Business Metrics / Measures
	-- ==========================================
    passenger_count          INT,     -- Total passengers in the trip
    trip_distance_miles      FLOAT,   -- Total distance traveled
    fare_amount_usd          FLOAT,   -- Base fare calculated by the meter
	tip_amount_usd           FLOAT,   -- Gratuity amount given to the driver
    total_amount_usd         FLOAT    -- Final total charged (including tips/tolls)
);
GO
