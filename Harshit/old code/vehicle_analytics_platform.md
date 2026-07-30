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
                       Vehicle Dataset
                             │
                             ▼
                 Raw Data Ingestion (CSV)
                             │
                             ▼
                       Hadoop HDFS
                             │
                             ▼
                    Bronze Layer (Raw Data)
                             │
                             ▼
                  Silver Layer (Clean Data)
                  (Spark: dedup, type fix,
                   outlier removal)
                             │
                             ▼
               Feature Engineering Layer
               (Vehicle Age, Segments,
                Price Categories, etc.)
                             │
                             ▼
             Gold Layer (Reporting Tables)
                             │
                             ▼
                   Hive Data Warehouse
                             │
                             ▼
                     Tableau Dashboards
```

### Real-Time Pipeline

```
Python Producer
      │
      ▼
   Kafka Topic
      │
      ▼
Spark Structured Streaming
      │
      ▼
 Live Aggregations
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

### Gold Layer (Business Data)
Business features are created for analytics including:

- Vehicle Age
- Mileage Buckets
- Price Categories
- Vehicle Segments
- Luxury Vehicle Classification
- Posting Month
- Posting Year
- Depreciation Indicators
- Regional Grouping

Aggregated reporting tables are also generated for Tableau.

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
- Produce reporting datasets

### Phase 5 — Hive Data Warehouse
- Load analytical tables into Hive
- Partition and optimize tables
- Execute SQL-based analytical queries

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
- Connect to Hive or exported Gold CSVs.
- Build the five dashboards.
- Add filters, actions, KPIs, and formatting.
- Validate dashboard metrics against Gold tables.
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


┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                         🚗 USED VEHICLE MARKET - EXECUTIVE DASHBOARD                         │
├─────────────┬─────────────┬─────────────┬─────────────┬──────────────────────────────────────┤
│ Total Cars  │ Avg Price   │ Avg Mileage│ Avg Age     │ Manufacturers                         │
│ 426,880     │ $18,250     │ 108K Miles │ 8.2 Years   │ 145                                  │
└─────────────┴─────────────┴─────────────┴─────────────┴──────────────────────────────────────┘


┌───────────────────────────────────────┬──────────────────────────────────────────────────────┐
│                                       │                                                      │
│        🗺 USA MAP                      │       📊 Monthly Marketplace Comparison             │
│     Average Price by State            │              April vs May                           │
│                                       │   April ██████████████████ 164,906                 │
│                                       │   May   ████████           78,073                  │
│                                       │                                                      │
└───────────────────────────────────────┴──────────────────────────────────────────────────────┘


┌─────────────────────────────┬──────────────────────────────┬─────────────────────────────────┐
│ Top Manufacturers           │ Fuel Distribution            │ Vehicle Condition               │
│ Horizontal Bar              │ Donut Chart                  │ Stacked Bar                     │
└─────────────────────────────┴──────────────────────────────┴─────────────────────────────────┘


Filters:
State | Manufacturer | Fuel | Transmission | Vehicle Type

**Key Metrics:**
- Total Listings
- Average Price
- Median Price
- Average Vehicle Age
- Average Mileage
- Number of Manufacturers

**Visualizations:**
- USA Market Map
- Monthly Listing Comparison
- Fuel Distribution
- Top Manufacturers
- Vehicle Condition Analysis

### Dashboard 2 — Price Intelligence
Focuses on pricing behavior across different market segments.


┌─────────────────────────────────────────────────────┐
│                  💰 PRICE INTELLIGENCE DASHBOARD    │
┌─────────────────────────────────────────────────────┐
│ Avg | Max | Min | Median | StdDev                   │
├──────────────────────┬──────────────────────────────┤
│ Avg Price            │ April vs May                 │
│ Manufacturer         │ Column Chart                 │
├──────────────────────┼──────────────────────────────┤
│ Fuel Type            │ Vehicle Condition            │
│ Lollipop             │ Dot Plot                     │
├──────────────────────┴──────────────────────────────┤
│ Manufacturer × Condition Heatmap                    │
├─────────────────────────────────────────────────────┤
│ Scatter Plot : Mileage vs Average Price             │
└─────────────────────────────────────────────────────┘

Filters:
Year | Manufacturer | Fuel | Condition | State

**Visualizations:**
- Average Price by Manufacturer
- Price Distribution
- Price by Fuel Type
- Price by Vehicle Condition
- Price by Transmission
- Heatmaps
- Top Premium Models

### Dashboard 3 — Vehicle Health & Inventory
Analyzes inventory characteristics and vehicle quality.


┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                        🚙 VEHICLE HEALTH & INVENTORY DASHBOARD                              │
├───────────────┬───────────────┬──────────────┬──────────────┬────────────────────────────────┤
│ Avg Mileage   │ Avg Vehicle   │ Automatic %  │ Clean Title  │ Salvage %                     │
│               │ Age           │              │              │                               │
└───────────────┴───────────────┴──────────────┴──────────────┴────────────────────────────────┘


┌────────────────────────────────────┬─────────────────────────────────────────────────────────┐
│ Mileage Distribution               │ Vehicle Age Distribution                               │
│ Histogram                          │ Histogram                                              │
└────────────────────────────────────┴─────────────────────────────────────────────────────────┘


┌────────────────────────────────────┬─────────────────────────────────────────────────────────┐
│ Price vs Mileage                   │ Price vs Vehicle Age                                  │
│ Scatter Plot                       │ Scatter Plot                                          │
└────────────────────────────────────┴─────────────────────────────────────────────────────────┘


┌─────────────────────────────┬──────────────────────────────┐
│ Vehicle Type                │ Drive Type                   │
│ Treemap                     │ Donut                        │
└─────────────────────────────┴──────────────────────────────┘


Filters:
Vehicle Type | Drive | Transmission | Condition

**Visualizations:**
- Mileage Distribution
- Vehicle Age Distribution
- Price vs Mileage
- Price vs Vehicle Age
- Drive Type Distribution
- Vehicle Type Analysis
- Title Status
- Condition Distribution

### Dashboard 4 — Geographic Market Analytics
Provides regional insights into the used vehicle market.

┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                        🌎 GEOGRAPHIC MARKET ANALYTICS                                        │
├───────────────┬──────────────┬───────────────┬──────────────┬────────────────────────────────┤
│ Total States  │ Avg Price    │ Highest State │ Lowest State │ Total Listings               │
└───────────────┴──────────────┴───────────────┴──────────────┴────────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                                                                              │
│                                🗺 Interactive USA Map                                        │
│                        Average Price / Listings / Mileage                                   │
│                                                                                              │
└──────────────────────────────────────────────────────────────────────────────────────────────┘


┌─────────────────────────────┬──────────────────────────────┐
│ State Rankings              │ Listings by State            │
│ Horizontal Bar              │ Tree Map                     │
└─────────────────────────────┴──────────────────────────────┘


┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│ Heatmap : State × Manufacturer × Average Price                                              │
└──────────────────────────────────────────────────────────────────────────────────────────────┘


Filters:
State | Manufacturer

**Visualizations:**
- Interactive USA Map
- State-wise Average Price
- State-wise Listing Count
- Manufacturer Popularity by State
- State Heatmaps
- Mileage by State

### Dashboard 5 — Real-Time Marketplace Monitor
Displays live marketplace activity using streaming analytics.

┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                      ⚡ REAL-TIME MARKETPLACE MONITOR (Streaming)                            │
├──────────────┬──────────────┬──────────────┬──────────────┬──────────────────────────────────┤
│ Live Listings│ Listings/min │ Avg Live $   │ Messages     │ Top Manufacturer                │
│              │              │              │ Processed    │ (Live)                          │
└──────────────┴──────────────┴──────────────┴──────────────┴──────────────────────────────────┘


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
- Latest Listings Table
- Top Manufacturers (Live Ranking)
- Fuel Distribution (Live Donut)
- Live State Distribution

---

## 13. Dashboard Validation

Each Tableau dashboard was validated against the Gold reporting tables generated using Apache Spark. KPI values, aggregations, and summary statistics displayed in Tableau were cross-verified with the corresponding Gold datasets to ensure analytical consistency throughout the pipeline.

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
Gold Layer
(Aggregated Reporting Tables for Tableau)

↓

Phase 5
Hive Data Warehouse
(Partitioning, Bucketing, OLAP Queries)

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
(5 Professional Dashboards connected to Gold + HBase)

↓

Phase 11
Deploy Locally → Migrate to AWS (EMR/EC2 + S3) or Databricks