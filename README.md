# 🚖 NYC Taxi Data Warehouse & Analytics Pipeline

Welcome to the **NYC Taxi Data Warehouse** repository! This project is an end-to-end Data Engineering pipeline designed to extract over 4 million trip records, integrate them with external weather and geographical data, and transform them through a highly scalable Medallion Architecture into a reporting-ready Star Schema.

## 🏗️ Data Architecture
The pipeline utilizes a multi-layer **Medallion Architecture** (Bronze, Silver, Gold) built on SQL Server, orchestrated via Python, and optimized for memory efficiency. 

*(Note: The full dataset including the 4M+ rows Parquet file and all lookup CSVs is available in the Releases section of this repository).*

### 1. High-Level Architecture
![High-Level Architecture](docs/data_architecture.png)

### 2. Data Flow Diagram (Table Lineage)
![Data Flow Diagram](docs/data_flow.png)

* **🥉 Bronze Layer (Raw Data):** Ingests raw data from diverse sources including Parquet files (NYC TLC trips), JSON via REST API (Open-Meteo weather), and CSV files (Taxi zones & US Holidays).
* **🥈 Silver Layer (Cleansed Data):** Performs data cleansing, handles missing values, filters invalid fares/distances, and standardizes data types.
* **🥇 Gold Layer (Star Schema):** Structures the data into a Fact and Dimension model (One-to-Many relationships) using Surrogate Keys, optimized via Chunked Inserts for massive datasets.
* **💎 Semantic Layer (Views):** Business-ready analytical views denormalizing the Star Schema to serve direct BI dashboards and Ad-Hoc SQL queries.

### 3. Entity-Relationship Diagram (ERD)
The Gold Layer is modeled using a Kimball-methodology Star Schema to optimize analytical queries:

![Star Schema ERD](docs/data_model.png)

## 💼 Project Overview & Engineering Highlights
This project demonstrates practical solutions to real-world Data Engineering challenges:
* **Data Integration:** Unified multiple data formats (`.parquet`, `.csv`, `JSON/API`) into a single centralized warehouse.
* **Memory Optimization:** Engineered Python scripts using `chunksize=10000` to smoothly load 4M+ rows without overwhelming system RAM.
* **Data Modeling:** Designed a Kimball-methodology Star Schema mapped with Entity-Relationship Diagrams (ERD).
* **Tech Stack:** `Python`, `Pandas`, `SQLAlchemy`, `SQL Server (T-SQL)`, `Draw.io`.

---

## 🛣️ Project Roadmap & Execution

### 🔍 1. Requirements & Source Analysis
- [x] Analyze NYC TLC Parquet Data Structure.
- [x] Connect and extract data from Open-Meteo REST API.
- [x] Gather Lookup CSV data (Zones, Holidays).

### 📐 2. Design Data Architecture
- [x] Design Medallion Architecture (Bronze, Silver, Gold).
- [x] Draft Data Flow Diagram (DFD) for the pipeline.
- [x] Draft Entity-Relationship Diagram (ERD) for the Star Schema.

### 🛠️ 3. Project Workspace
- [x] Create GitHub Repository & configure `.gitignore`.
- [x] Setup Python Virtual Environment and dependencies.

### 🥉 4. Build Bronze Layer (Extraction)
- [x] Extract Parquet, CSV, and API data using Pandas.
- [x] Load Raw Data into SQL Server.

### 🥈 5. Build Silver Layer (Transformation)
- [x] Cleanse Data (Filter anomalous trip distances and fares).
- [x] Standardize column names to `snake_case`.
- [x] Data Type Casting and formatting.

### 🥇 6. Build Gold Layer (Data Modeling)
- [x] Generate Surrogate Keys and Foreign Key relationships.
- [x] Load Dimension Tables (Full Load).
- [x] Load Fact Table (Incremental / Chunked Inserts).

### 💎 7. Semantic Layer & Analytics
- [x] Create `vw_trip_analysis_master` for BI Reporting.
- [x] Create `vw_weather_impact_summary` to correlate trips with weather conditions.
- [x] Create `vw_location_performance` for spatial analysis.

---

## 🛡️ License
This project is licensed under the MIT License - see the LICENSE file for details.

## 👨‍💻 About Me
Hello, I'm **Ahmed Samy Abdullah Ali**, an Engineering Student specializing in Communications and Electronics, and an aspiring **Data Engineer**. 

I am deeply focused on database architectures, data modeling, and building automated ETL data pipelines. I work with tools like Python, SQL, and data processing libraries to build structured and scalable data solutions. This project is a practical implementation of memory management, dimensional modeling, and end-to-end data pipeline orchestration.

Let's connect and talk about Data! 🚀

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/ahmed-samy-009b38387?utm_source=share_via&utm_content=profile&utm_medium=member_android) 
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/ahmedsamy284) 
[![Email](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:ahmedahmed01026378757@gmail.com)
