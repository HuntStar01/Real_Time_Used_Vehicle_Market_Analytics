# Real-Time Used Vehicle Market Analytics Platform
**Big Data Engineering + Data Warehousing + Streaming Analytics + Tableau Business Intelligence**

---

## 1. Project Overview

The rapid growth of online automobile marketplaces has resulted in massive volumes of vehicle listing data generated every day. Millions of buyers and sellers interact through platforms such as Craigslist, producing valuable information regarding vehicle pricing, market trends, geographical demand, depreciation, inventory distribution, and consumer preferences.

Traditional data processing systems struggle to efficiently analyze such large-scale datasets while simultaneously supporting historical analytics and real-time monitoring.

This project aims to build an end-to-end Big Data Analytics Platform capable of processing both historical and streaming vehicle listing data using distributed computing technologies. The project combines batch processing, real-time streaming, data warehousing, NoSQL storage, and business intelligence dashboards into a unified analytics pipeline.

The final system enables users to explore market trends, pricing behavior, vehicle characteristics, regional demand, and live marketplace activity through interactive Tableau dashboards.

---

## 2. Background & Dataset Definition

### Dataset

**Craigslist Cars and Trucks Dataset**

**Dataset Source:** https://www.kaggle.com/datasets/austinreese/craigslist-carstrucks-data

### Dataset Description

The dataset consists of vehicle listings collected from Craigslist across different regions of the United States.

Each listing contains detailed information about the vehicle including:

- Manufacturer
- Model
- Price
- Year
- Fuel Type
- Odometer Reading
- Vehicle Condition
- Transmission
- Drive Type
- Paint Color
- Vehicle Size
- Geographic Location
- Posting Date
- Description
- Images
- VIN Number

### Dataset Size

- Approximately 1.45 GB
- Approximately 426,000+ vehicle listings
- 26 attributes

The combination of structured, categorical, numerical, geographical, and textual attributes makes this dataset highly suitable for demonstrating Big Data processing and visualization techniques.

---

## 3. Background

The automotive market generates enormous volumes of transactional and listing data every day. Buyers use these listings to evaluate pricing, compare vehicles, understand depreciation, and identify regional market trends.

Organizations managing such platforms require scalable systems capable of:

- Processing large historical datasets
- Performing large-scale ETL operations
- Supporting analytical querying
- Handling continuously arriving listings
- Delivering business insights through dashboards

Modern Big Data technologies such as Hadoop, Spark, Kafka, Hive, and HBase enable organizations to efficiently manage these requirements.

This project demonstrates how these technologies can be integrated into a complete analytics platform.

---

## 4. The Five Vs of Big Data

### Volume
The dataset contains approximately 1.45 GB of vehicle listing data with hundreds of thousands of records and millions of individual attribute values. The architecture is designed to scale for much larger datasets by leveraging Hadoop Distributed File System (HDFS).

### Velocity
Although the original dataset is static, real-world vehicle marketplaces continuously receive new listings. To simulate this scenario, Kafka producers generate streaming vehicle records that are processed using Spark Structured Streaming.

### Variety
The dataset contains multiple data types including:

- Numerical Data
- Categorical Data
- Text Data
- Geographical Coordinates
- URLs
- Timestamp Data

This variety requires flexible ETL techniques and distributed storage.

### Veracity
The dataset contains several data quality issues including:

- Missing values
- Inconsistent formatting
- Duplicate records
- Invalid prices
- Invalid odometer readings
- Missing manufacturer information

A complete Spark-based ETL pipeline is developed to improve data quality before analysis.

### Value
After processing, the data provides valuable business insights including:

- Vehicle price trends
- Market demand
- Manufacturer popularity
- Geographic distribution
- Vehicle depreciation
- Fuel preferences
- Inventory analysis
- Live marketplace activity

---

## 5. Problem Statement

Vehicle marketplace data is large, heterogeneous, and continuously growing.

Traditional analytical approaches are insufficient for handling large-scale historical data while simultaneously supporting real-time analytics.

The absence of scalable ETL pipelines, distributed storage, and interactive business intelligence limits the ability to generate meaningful insights for decision-makers.

The objective of this project is to design and implement a complete Big Data Analytics Platform capable of processing both historical and streaming vehicle listing data using modern distributed technologies.

---

## 6. Project Objectives

The primary objectives of this project are:

1. Build an end-to-end Big Data Analytics Platform.
2. Design a scalable data ingestion pipeline.
3. Store raw data using Hadoop Distributed File System (HDFS).
4. Perform distributed ETL using Apache Spark.
5. Improve data quality through cleaning and preprocessing.
6. Engineer meaningful business features for analytics.
7. Store analytical datasets in Hive Data Warehouse.
8. Simulate real-time vehicle listing ingestion using Kafka.
9. Process streaming data using Spark Structured Streaming.
10. Store operational streaming data in HBase.
11. Design interactive Tableau dashboards.
12. Generate business insights for pricing, inventory, geographical trends, and market behavior.

---

## 7. Project Scope

The project covers both historical and streaming analytics.

### Historical Analytics
- Data Cleaning
- Feature Engineering
- Aggregations
- Business Reporting
- Interactive Dashboards

### Streaming Analytics
- Kafka Data Streaming
- Spark Streaming
- Live Market Metrics
- Streaming Dashboard

---

## 8. Technologies Used

| Technology | Purpose |
|---|---|
| Hadoop HDFS | Distributed Storage |
| Apache Spark (PySpark) | Distributed ETL & Batch Processing |
| Apache Hive | Data Warehouse & SQL Analytics |
| Apache Kafka | Real-Time Data Streaming |
| Spark Structured Streaming | Streaming Analytics |
| Apache HBase | NoSQL Storage for Live Data |
| Tableau Public | Business Intelligence & Visualization |
| Python | Data Processing & Kafka Producer |
| Git & GitHub | Version Control |
| macOS Local Environment | Development Platform |

---

## 9. Proposed System Architecture

### Batch Pipeline

```
                       CSV Dataset
                             │
                             ▼
                       HDFS Bronze
                             │
                             ▼
               01_Bronze_to_Silver_ETL
                             │
                             ▼
               03_Feature_Engineering
                             │
                             ▼
           Featured Dataset (Parquet)
                             │
                             ▼
              05_Hive_Data_Warehouse
                             │
                  ┌──────────┴──────────┐
                  ▼                     ▼
        Hive External Table    Tableau CSV Export
                             │
                             ▼
                    Tableau Dashboards
```

### Real-Time Pipeline

```
Featured Dataset
      │
      ▼
Kafka Producer
      │
      ▼
Spark Structured Streaming
      │
      ▼
    HBase
      │
      ▼
Real-Time Tableau Dashboard
```

---

## 10. Data Engineering Workflow

The project follows a layered Data Engineering architecture inspired by modern data lake implementations.

### Bronze Layer (Raw Data)
The original dataset is stored in Hadoop Distributed File System without any modifications.

**Purpose:**
- Data preservation
- Data lineage
- Recovery
- Raw archival storage

### Silver Layer (Clean Data)
Spark performs data cleaning operations including:

- Data type conversion
- Missing value handling
- Duplicate removal
- Outlier detection
- Invalid record filtering
- Standardization of categorical values

This layer produces a high-quality analytical dataset.

### Feature Engineering Layer
The Feature Engineering layer produces the final analytical dataset. Hive registers this dataset as an external table, and Tableau performs business aggregations, KPI calculations, filtering, and dashboard generation directly from this dataset. No intermediate Gold summary tables are maintained.

---

## 11. Step-by-Step Project Flow

### Phase 0 — Dataset Profiling
- Understand dataset structure
- Analyze schema
- Identify missing values
- Study attribute distributions
- Perform data quality assessment

### Phase 1 — Data Cleaning
- Convert data types
- Remove duplicates
- Handle missing values
- Correct inconsistent data
- Validate geographical coordinates
- Remove unrealistic records

### Phase 2 — Feature Engineering
- Create business attributes
- Derive analytical metrics
- Build categorical groupings
- Generate time-based features

### Phase 3 — Hadoop Storage
- Upload raw dataset into HDFS
- Organize distributed storage
- Maintain raw data repository

### Phase 4 — Spark ETL
- Read data from HDFS
- Perform distributed transformations
- Generate clean datasets
- Produce Feature Engineering Dataset

### Phase 5 — Hive Data Warehouse
- Register the engineered dataset as a single external table (`vehicle_featured`)
- Execute SQL queries for validation and exploratory analytics
- Export a single CSV for Tableau

### Phase 6 — Kafka Streaming
- Simulate continuous vehicle listings
- Publish records into Kafka topics

### Phase 7 — Spark Structured Streaming
- Consume Kafka streams
- Perform live aggregations
- Compute real-time marketplace metrics

### Phase 8 — HBase Storage
- Store latest streaming summaries
- Maintain operational analytics tables

### Phase 9 — Apache Airflow Orchestration
- Install and configure Airflow.
- Create a DAG for the complete pipeline.
- Execute all phases in order.
- Configure retries, logging, and dependencies.
- Validate DAG execution.
- Capture Airflow screenshots for the report.

### Phase 10 — Tableau Dashboards
- Connect Tableau to the exported Feature Engineering CSV generated from the Hive external table.
- Build the six dashboards.
- Add filters, actions, KPIs, and formatting.
- Validate dashboard metrics against Hive SQL query results and the exported Feature Engineering dataset.
- Prepare screenshots and the final Tableau workbook.

<!-- ### Phase 10 — Tableau Visualization
- Connect Tableau with reporting datasets
- Develop interactive dashboards
- Publish business insights -->

---

## 12. Data Visualization & Dashboards

The visualization layer is designed to provide both strategic and operational insights.

### Dashboard 1 — Executive Market Overview
Provides a high-level summary of the used vehicle marketplace.

```
┌────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                         🚗 USED VEHICLE MARKET - EXECUTIVE OVERVIEW                                        │
├────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Filters                                                                           Reset Filters            │
│ Manufacturer ▼   State ▼   Fuel ▼   Transmission ▼   Price Category ▼   Segment ▼                        │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────┬────────────┬────────────┬────────────┬────────────┬────────────┬────────────┬──────────────┐
│ Total Cars │ Avg Price  │ Median $   │ Avg Age    │ Avg Miles  │ Luxury %   │ Automatic% │ Alt Fuel %   │
└────────────┴────────────┴────────────┴────────────┴────────────┴────────────┴────────────┴──────────────┘

┌──────────────────────────────────────────┬───────────────────────────────────────────────────────────────┐
│                                          │                                                               │
│         🗺 USA MAP                        │          📈 Listings by Month                                │
│     Avg Price by State                   │          Line Chart                                           │
│                                          │                                                               │
└──────────────────────────────────────────┴───────────────────────────────────────────────────────────────┘

┌──────────────────────────────┬──────────────────────────────┬────────────────────────────────────────────┐
│ Top Manufacturers            │ Fuel Distribution            │ Vehicle Segment                            │
│ Horizontal Bar               │ Donut                        │ Treemap                                   │
└──────────────────────────────┴──────────────────────────────┴────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Price Category Distribution (Budget • Economy • Mid • Premium • Luxury)                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Interactivity:**
- Click a state on the map → all KPIs and charts update for that state
- Click a manufacturer → all KPIs update to reflect that manufacturer
- Click a price category → dashboard filters to that segment

**Key Metrics:** Total Listings, Average Price, Median Price, Average Vehicle Age, Average Mileage, Luxury % (`is_luxury`), Automatic % (`is_automatic`), Alternative Fuel % (`is_alternative_fuel`)

**Visualizations:**
- USA Map — Avg Price by State
- Listings by Month — Line Chart
- Top Manufacturers — Horizontal Bar
- Fuel Distribution — Donut
- Vehicle Segment — Treemap
- Price Category Distribution — Stacked Bar

### Dashboard 2 — Price Intelligence
Focuses on pricing behavior across different market segments.

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                💰 PRICE INTELLIGENCE DASHBOARD                                           │
├──────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Manufacturer ▼  Year ▼  Fuel ▼  Condition ▼  State ▼  Segment ▼                                         │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────┬────────────┬────────────┬────────────┬────────────┐
│ Avg Price  │ Median $   │ Max Price  │ Min Price  │ Avg PPM    │
└────────────┴────────────┴────────────┴────────────┴────────────┘

┌─────────────────────────────────────────────┬───────────────────────────────────────────────────────────┐
│ Average Price by Manufacturer               │ Price Category Distribution                               │
│ Horizontal Bar                              │ Stacked Bar                                               │
└─────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────┬───────────────────────────────────────────────────────────┐
│ Price per Mile Scatter Plot                 │ Depreciation Index by Manufacturer                        │
└─────────────────────────────────────────────┴───────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Heatmap : Manufacturer × Condition × Average Price                                                      │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Engineered Features Used:** `price_per_mile`, `price_category`, `price_band`, `depreciation_index`

**Visualizations:**
- Average Price by Manufacturer — Horizontal Bar
- Price Category Distribution (Budget / Economy / Mid-Range / Premium / Luxury) — Stacked Bar using `price_category`
- Price per Mile Scatter Plot using `price_per_mile`
- Depreciation Index by Manufacturer using `depreciation_index`
- Manufacturer × Condition × Average Price — Heatmap

### Dashboard 3 — Vehicle Health & Inventory
Analyzes inventory characteristics and vehicle quality.

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                          🚙 VEHICLE HEALTH & INVENTORY                                                   │
├──────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Vehicle Type ▼  Segment ▼  Transmission ▼  Condition ▼                                                  │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌──────────────┬──────────────┬──────────────┬──────────────┬──────────────┐
│ Avg Mileage  │ Avg Age      │ Automatic %  │ Clean Title% │ Salvage %    │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────┘

┌──────────────────────────────────────┬────────────────────────────────────────┐
│ Vehicle Age Groups                   │ Mileage Category                       │
│ Histogram                            │ Histogram                              │
└──────────────────────────────────────┴────────────────────────────────────────┘

┌──────────────────────────────────────┬────────────────────────────────────────┐
│ Price vs Vehicle Age                 │ Price vs Mileage                       │
│ Scatter                              │ Scatter                                │
└──────────────────────────────────────┴────────────────────────────────────────┘

┌───────────────────────────────┬──────────────────────────────────────────────┐
│ Vehicle Segment               │ Drive Type                                  │
│ Treemap                       │ Donut                                       │
└───────────────────────────────┴──────────────────────────────────────────────┘
```

**Engineered Features Used:** `vehicle_age`, `age_group`, `mileage_category`, `vehicle_segment`, `clean_title`, `is_salvage`, `is_automatic`

**Visualizations:**
- Vehicle Age Groups — Histogram using `age_group` (0–3 / 4–7 / 8–12 / 13+)
- Mileage Category — Histogram using `mileage_category` (Low / Medium / High / Very High)
- Price vs Vehicle Age — Scatter
- Price vs Mileage — Scatter
- Vehicle Segment — Treemap using `vehicle_segment` (Economy / Standard / Premium / Luxury)
- Drive Type — Donut
- Clean Title % and Salvage % KPIs

### Dashboard 4 — Geographic Market Analytics
Provides regional insights into the used vehicle market.

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                            🌎 GEOGRAPHIC MARKET ANALYTICS                                                │
├──────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ State ▼  Manufacturer ▼  Fuel ▼                                                                         │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────┬────────────┬────────────┬────────────┐
│ Avg Price  │ Listings   │ Avg Age    │ Avg Dep.   │
└────────────┴────────────┴────────────┴────────────┘

┌────────────────────────────────────────────┬────────────────────────────────────────────────────────────┐
│               USA MAP                      │      State Ranking                                         │
│ Average Price / Listings                   │ Horizontal Bar                                             │
└────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────┬────────────────────────────────────────────────────────────┐
│ Luxury Vehicle Distribution                │ Alternative Fuel Distribution                              │
│ Choropleth / Filled Map                    │ Filled Map                                                 │
└────────────────────────────────────────────┴────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Heatmap : State × Manufacturer × Average Price                                                         │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Engineered Features Used:** `is_luxury`, `depreciation_index`, `is_alternative_fuel`

**Visualizations:**
- USA Map — Average Price / Listings by State
- State Ranking — Horizontal Bar
- Luxury Vehicle Distribution by State — Choropleth using `is_luxury`
- Alternative Fuel Adoption by State — Filled Map using `is_alternative_fuel`
- Average Depreciation Index by State using `depreciation_index`
- State × Manufacturer × Average Price — Heatmap

### Dashboard 5 — Market Activity & Time Analytics
Analyzes when listings are posted and tracks temporal patterns across hours, periods, days, weeks, and seasons.

```
┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                              ⏰ MARKET ACTIVITY & TIME ANALYTICS                                         │
├──────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ Year ▼  Manufacturer ▼  State ▼                                                                         │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌────────────┬────────────┬────────────┬────────────┬────────────┐
│ Weekend %  │ Avg Price  │ Avg Age    │ Avg PPM    │ Listings   │
└────────────┴────────────┴────────────┴────────────┴────────────┘

┌──────────────────────────────────────────┬──────────────────────────────────────────────────────────────┐
│ Listings by Hour                         │ Listings by Posting Period                                  │
│ Line Chart                               │ Morning / Afternoon / Evening / Night                       │
└──────────────────────────────────────────┴──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────┬──────────────────────────────────────────────────────────────┐
│ Listings by Season                       │ Weekend vs Weekday                                          │
│ Bar Chart                                │ Donut                                                       │
└──────────────────────────────────────────┴──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Month-wise Trend                                                                        Quarter Trend    │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Engineered Features Used:** `posting_hour`, `posting_period`, `posting_season`, `posting_month_name`, `posting_quarter`, `posting_weekday_name`, `is_weekend`

**Visualizations:**
- Listings by Hour — Line Chart using `posting_hour`
- Listings by Posting Period — Bar using `posting_period` (Morning / Afternoon / Evening / Night)
- Listings by Season — Bar Chart using `posting_season`
- Weekend vs Weekday — Donut using `is_weekend`
- Month-wise Trend using `posting_month_name`
- Quarter-wise Trend using `posting_quarter`

---

### Dashboard Navigation

A navigation bar appears at the top of every dashboard for one-click switching:

```
┌────────────────────────────────────────────────────────────────────────────────────────────┐
│ 🚗 Executive │ 💰 Price │ 🚙 Vehicle │ 🌎 Geographic │ ⏰ Time Analytics │ ⚡ Real-Time  │
└────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Global Filters (Applied Across All Dashboards)

- Manufacturer
- State
- Model Year
- Fuel Type
- Transmission
- Condition
- Vehicle Segment
- Price Category
- Age Group

### Cross-Filtering (Interactive)

Tableau filter actions cascade across all charts on a dashboard:

```
Dashboard 1

Click: Toyota
      ↓
Dashboard updates:
  • KPI Cards
  • USA Map
  • Fuel Distribution
  • Price Category
  • Vehicle Segment
      ↓
Click: California
      ↓
Now only Toyota vehicles in California
      ↓
Click: Premium
      ↓
Now only Premium Toyota vehicles in California
```

All updates occur instantly through Tableau's filter actions.

---

### Dashboard 6 — Real-Time Marketplace Monitor
Displays live marketplace activity using streaming analytics.

┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                      ⚡ REAL-TIME MARKETPLACE MONITOR (Streaming)                            │
├──────────┬──────────┬──────────┬──────────┬──────────┬──────────┬──────────────────────────────┤
│ Live     │ Listings │ Avg Live │ Messages │ Top Mfr  │ Live Avg │ Live Avg Price              │
│ Listings │ /min     │ Price $  │ Processed│ (Live)   │ Veh Age  │ per Mile                    │
└──────────┴──────────┴──────────┴──────────┴──────────┴──────────┴──────────────────────────────┘


┌────────────────────────────────────┬─────────────────────────────────────────────────────────┐
│                                    │                                                         │
│ Live USA Map                       │ Live Listing Trend                                     │
│ New Listings                       │ Streaming Line Chart                                  │
│                                    │                                                         │
└────────────────────────────────────┴─────────────────────────────────────────────────────────┘


┌───────────────────────────────┬──────────────────────────────┬───────────────────────────────┐
│ Latest Listings               │ Top Manufacturers            │ Fuel Distribution             │
│ Live Table                    │ Live Ranking                │ Live Donut                    │
└───────────────────────────────┴──────────────────────────────┴───────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│ Producer Status | Consumer Status | Messages Processed | Throughput | Last Refresh          │
└──────────────────────────────────────────────────────────────────────────────────────────────┘

**Visualizations:**
- Live Listing Counter
- Listings per Minute
- Messages Processed
- Average Live Price
- Live Average Vehicle Age (`vehicle_age`)
- Live Average Price per Mile (`price_per_mile`)
- Latest Listings Table
- Top Manufacturers (Live Ranking)
- Fuel Distribution (Live Donut)
- Live State Distribution

---

## 13. Dashboard Validation

Each Tableau dashboard was validated against Hive SQL query results and the exported Feature Engineering dataset. KPI values, aggregations, and summary statistics displayed in Tableau were cross-verified with the corresponding Hive outputs to ensure analytical consistency throughout the pipeline.

---

## 14. Expected Outcomes

Upon completion, the project will demonstrate:

- End-to-end Big Data pipeline implementation
- Distributed data storage and processing
- Batch and streaming analytics
- Data warehousing concepts
- Interactive business intelligence dashboards
- Real-time marketplace monitoring
- Practical application of Hadoop ecosystem technologies
- Scalable architecture suitable for enterprise-level analytics

---

## 15. Future Enhancements

Although the initial implementation will be developed in a local environment, the architecture is designed for seamless migration to cloud platforms.

Possible future enhancements include:

- Deployment on AWS (Amazon EMR, EC2, S3)
- Databricks-based Spark execution
- Apache Airflow workflow orchestration
- Delta Lake for ACID-compliant data storage
- Real-time dashboard auto-refresh
- Predictive pricing models using Machine Learning
- REST APIs for dashboard integration
- Docker containerization and Kubernetes deployment
- CI/CD pipeline for automated deployments

### The last thought of the flow

Phase 0  ✅ Dataset Profiling
          ├── Schema validation
          ├── Missing value analysis
          ├── Duplicate analysis
          ├── Data type conversion
          ├── Outlier detection
          └── Data quality report

↓

Phase 1
Bronze Layer
(Raw Data → HDFS)

↓

Phase 2
Silver Layer
(Spark Cleaning: deduplication, type conversion, outlier removal)

↓

Phase 3
Feature Engineering
(Vehicle Age, Mileage Buckets, Price Categories, Vehicle Segments)

↓

Phase 4
Feature Engineering
(Spark ETL → Featured Dataset in Parquet)

↓

Phase 5
Hive Data Warehouse
(Single External Table → SQL Validation → CSV Export)

↓

Phase 6
Kafka Producer
(Simulated Live Vehicle Listings)

↓

Phase 7
Spark Structured Streaming
(Live Aggregations)

↓

Phase 8
HBase
(Store Live Streaming Metrics)

↓

Phase 9
Apache Airflow
(Pipeline Orchestration & DAG Execution)

↓

Phase 10
Tableau Dashboards
(6 Professional Dashboards connected to Feature Engineering CSV + HBase)

↓

Phase 11
Deploy Locally → Migrate to AWS (EMR/EC2 + S3) or Databricks