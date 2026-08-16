# Real-Time Used Vehicle Market Analytics Platform

**Big Data Engineering · Data Warehousing · Streaming Analytics · Tableau Business Intelligence**

---

## 1. Project Overview

The rapid growth of online automobile marketplaces has resulted in massive volumes of vehicle listing data generated every day. Millions of buyers and sellers interact through platforms such as Craigslist, producing valuable information regarding vehicle pricing, market trends, geographical demand, depreciation, inventory distribution, and consumer preferences.

Traditional data processing systems struggle to efficiently analyze such large-scale datasets while simultaneously supporting historical analytics and real-time monitoring.

This project builds an end-to-end Big Data Analytics Platform capable of processing both historical and streaming vehicle listing data using distributed computing technologies. The platform follows a **Medallion Architecture** (Bronze → Silver → Featured → Gold) and combines two parallel tracks:

- **Batch Track** — Apache Spark on Hadoop HDFS, Apache Hive Data Warehouse, Apache Airflow orchestration
- **Streaming Track** — Databricks Delta Live Tables (DLT), Confluent Cloud Kafka, Spark Structured Streaming

The final system enables users to explore market trends, pricing behaviour, vehicle characteristics, regional demand, and live marketplace activity through interactive Tableau dashboards.

---

## 2. Dataset

**Craigslist Cars and Trucks Dataset**

**Source:** https://www.kaggle.com/datasets/austinreese/craigslist-carstrucks-data

### Description

The dataset consists of vehicle listings collected from Craigslist across different regions of the United States. Each listing contains detailed information about the vehicle including:

- Manufacturer, Model, Year
- Price, Odometer Reading
- Fuel Type, Transmission, Drive Type
- Vehicle Condition, Vehicle Size, Paint Colour
- Title Status, VIN Number
- Geographic Location (State, Region, Latitude, Longitude)
- Posting Date, Description, Image URL

### Size

| Metric | Value |
|---|---|
| Raw dataset size | ~1.45 GB |
| Raw listings | ~426,000+ records |
| Attributes | 26 columns |
| Records after cleaning & filtering | ~242,979 listings |
| US States covered | 50 |
| Manufacturers present | 43 |

The combination of structured, categorical, numerical, geographical, and textual attributes makes this dataset suitable for demonstrating Big Data processing, feature engineering, and business intelligence techniques.

---

## 3. The Five Vs of Big Data

### Volume
The raw dataset contains approximately 1.45 GB of vehicle listing data with 426,000+ records across 50 US states and 43 manufacturers. After cleaning, 242,979 valid listings remain. The architecture leverages HDFS for distributed storage and Parquet columnar format for efficient downstream querying.

### Velocity
Vehicle marketplaces continuously receive new listings. To handle this, a parallel **Databricks DLT streaming pipeline** is built alongside the batch track. Confluent Cloud Kafka acts as the message bus, and Spark Structured Streaming processes incoming records in near real time, updating Gold-layer aggregations continuously.

### Variety
The dataset spans multiple data types:
- Numerical (price, odometer, latitude, longitude)
- Categorical (manufacturer, fuel, transmission, condition, drive)
- Textual (description, model)
- Temporal (posting_date)
- URL / Identifier (image_url, VIN, url)

This variety requires flexible ETL techniques, explicit schema definitions, and type-aware transformations across both the batch and streaming pipelines.

### Veracity
The dataset contains significant data quality issues:
- Missing values across critical fields (manufacturer, odometer, transmission)
- Inconsistent categorical formatting (e.g., `"GAS"` vs `"gas"` vs `"Gas"`)
- Invalid prices (`0`, negative, unrealistically high)
- Invalid year values (outside 1980–present range)
- Duplicate listings (same manufacturer + model + year + price + odometer)
- Missing manufacturer information where only model name is available

A comprehensive Spark-based ETL pipeline with cascading imputation strategies and rule-based validation is developed to resolve these issues.

### Value
After processing, the data provides actionable business insights including:
- Vehicle price distribution and outlier detection
- Manufacturer performance and market share by region
- Regional supply and geographic demand patterns
- Fuel type and transmission mix across markets
- Vehicle depreciation trends by age and condition
- Posting behaviour patterns (time of day, day of week, seasonal trends)
- Real-time market KPIs via streaming aggregations

---

## 4. Problem Statement

Vehicle marketplace data is large, heterogeneous, continuously growing, and inherently noisy. Craigslist listings frequently contain missing values, inconsistent formatting, invalid prices, and duplicate records — making raw data unsuitable for direct analysis.

Traditional analytical approaches are insufficient for:
- Handling 1.45 GB+ historical data with distributed processing
- Simultaneously supporting near real-time analytics as new listings arrive
- Providing business-ready, query-optimised data for Tableau dashboards

The objective of this project is to design and implement a complete Big Data Analytics Platform that addresses these challenges through a Medallion Architecture, a parallel streaming pipeline, Hive data warehousing, Airflow orchestration, and interactive Tableau dashboards.

---

## 5. Project Objectives

1. Build an end-to-end Big Data Analytics Platform following Medallion Architecture.
2. Design a scalable Bronze ingestion layer on HDFS.
3. Perform distributed ETL and cleaning using Apache Spark (PySpark).
4. Apply multi-strategy imputation for missing values using Spark Window functions.
5. Engineer 13+ meaningful business features for analytics.
6. Register the Featured dataset as a Hive External Table in the Gold layer.
7. Automate the full batch pipeline using Apache Airflow DAGs.
8. Build a parallel real-time streaming pipeline on Databricks using DLT and Confluent Kafka.
9. Process and quality-score streaming data using Spark Structured Streaming.
10. Compute real-time Gold aggregations for live market KPIs.
11. Design interactive Tableau dashboards for business intelligence.
12. Generate insights for pricing, inventory, geographic trends, and market behaviour.

---

## 6. Project Scope

### Batch Analytics (Implemented)
- Bronze ingestion from HDFS
- Silver cleaning and transformation via Spark
- Feature engineering — 13 derived analytical columns
- Hive External Table registration (Gold layer)
- SQL validation queries on Hive
- CSV export for Tableau connectivity
- Apache Airflow DAG orchestration

### Streaming Analytics (Implemented)
- Databricks Volume as CSV landing zone
- Auto Loader (DLT) for incremental Bronze ingestion
- Confluent Cloud Kafka as message bus (SASL/SSL authentication)
- DLT consumer pipeline for Silver cleaning and quality scoring
- Real-time Gold aggregations (manufacturer × state × price_quality_status)

### Business Intelligence (Implemented)
- Executive Market Overview Dashboard
- Price Intelligence Dashboard

---

## 7. Technologies Used

| Technology | Purpose |
|---|---|
| Hadoop HDFS | Distributed Bronze and Silver layer storage |
| Apache Spark / PySpark | Distributed batch ETL, cleaning, feature engineering |
| Apache Hive | Gold layer Data Warehouse — external table registration |
| Databricks | Cloud Spark environment for streaming DLT pipelines |
| Delta Live Tables (DLT) | Declarative streaming pipeline framework on Databricks |
| Confluent Cloud Kafka | Managed Kafka — real-time message bus between producer and consumer |
| Spark Structured Streaming | Real-time consumer pipeline and Gold aggregations |
| Apache Airflow | Batch pipeline orchestration, DAG scheduling, SLA management |
| Apache Parquet | Columnar storage format for Silver, Featured, and Gold layers |
| Python (PySpark) | Data processing, transformations, Kafka payload formatting |
| Tableau Public | Business Intelligence dashboards and KPI visualisation |
| Git & GitHub | Version control |

---

## 8. System Architecture

### 8.1 Batch Pipeline (Hadoop / Hive / Airflow)

```
[Craigslist CSV Dataset]
         │
         ▼
[HDFS Bronze Layer]
(Raw CSV — immutable, no modifications)
         │
         ▼
[demo-eda.py — Silver ETL via Spark]
 • Schema normalisation & type casting
 • Categorical standardisation
 • Manufacturer inference (model-keyword lookup)
 • Cascading odometer imputation (window medians)
 • Transmission mode imputation (window frequency rank)
 • State code → full name (broadcast join)
 • Price & year business rule filters
         │
         ▼
[HDFS Silver Layer — Parquet, partitioned by state_name]
         │
         ▼
[demo-gold.py — Feature Engineering via Spark]
 • vehicle_age, age_group
 • price_category, mileage_category
 • price_per_mile, depreciation_index
 • Posting date/time features
 • Boolean flags (is_luxury, is_alternative_fuel, is_automatic, is_salvage)
 • vehicle_segment
         │
         ▼
[HDFS Featured/Gold Layer — Parquet]
         │
         ├──────────────────────────────┐
         ▼                              ▼
[03_Hive_Data_Warehouse.ipynb]   [CSV Export — coalesce(1)]
 Hive External Table                    │
 (vehicles_db.gold_vehicle)             ▼
         │                      [Tableau Dashboards]
         ▼
[SQL Validation Queries]
         │
   [Apache Airflow DAG]
   Silver Job >> Gold Job
   (daily schedule, retries, SLA)
```

### 8.2 Streaming Pipeline (Databricks / Confluent Kafka)

```
[CSV Files on Databricks Volume]
         │
         ▼ (cloudFiles — Auto Loader)
[Bronze DLT Table: gdrive_bronze_raw]
 • Incremental file detection
 • multiLine CSV with PERMISSIVE mode
         │
         ▼ (dlt.append_flow → dlt.create_sink)
[Confluent Cloud Kafka Topic: gdrive-csv-stream-topic]
 • key = vehicle id
 • value = full row as JSON
 • SASL/SSL authentication
         │
         ▼ (Spark Structured Streaming — startingOffsets: latest)
[Silver DLT Table: vehicles_confluent_silver_dlt]
 • from_json() with explicit StructType (25 fields)
 • fillna with domain-appropriate defaults
 • price_quality_status (ZERO_PRICE / SUSPECT_LOW / SUSPECT_HIGH / VALID)
 • listing_quality_score (0–5 completeness score)
         │
         ▼ (dlt.read_stream)
[Gold DLT Table: vehicles_gold_analytics]
 groupBy(manufacturer, state, price_quality_status)
 • total_active_listings
 • average_market_price
 • average_mileage
 • last_updated_timestamp
```

---

## 9. Data Engineering Workflow

The project follows a layered Medallion Architecture.

### Bronze Layer (Raw Data)
The original Craigslist CSV is stored in HDFS without any modifications.

**Purpose:** Data preservation, lineage, recovery, raw archival storage.

**Implementation:** `spark.read.csv()` with `mode="PERMISSIVE"`, `multiLine=True`, `quote='"'`, `escape='"'` to handle the messy multi-line records in Craigslist data.

### Silver Layer (Cleaned Data)
Spark performs comprehensive data cleaning. All columns are initially read as `StringType` to avoid parsing failures, then explicitly cast after cleaning.

**Transformations applied:**

| Operation | Detail |
|---|---|
| Type casting | lat/long → double, posting_date → timestamp, price/year/odometer → numeric |
| Drop useless columns | `county` dropped — near-entirely null, no analytical value |
| Category B null fill | size, condition, cylinders, drive, paint_color, type → `"unknown"` |
| Manufacturer inference | Model-keyword lookup dict (70+ entries) via Spark `when/contains` chains |
| Odometer imputation | Cascading coalesce: model median → manufacturer median → global median (Window functions) |
| Transmission imputation | Mode per manufacturer+model via window frequency ranking; fallback to `"automatic"` |
| State enrichment | Broadcast join with 51-row lookup: `"CA"` → `"California"` |
| Price filter | price > 0 required |
| Year filter | year between 1980 and current_year + 1 |
| Duplicate reporting | Fuzzy duplicates (same mfr+model+year+price+odo) reported, not dropped |

**Output:** Parquet on HDFS, partitioned by `state_name`.

### Featured / Gold Layer (Engineered Features)
Reads the Silver layer and adds 13 derived business features.

| Feature | Description |
|---|---|
| `vehicle_age` | current_year − listing year |
| `age_group` | Bucketed: Nearly New (0–2), Recent (3–5), Mid-Age (6–10), Older (11–15), Classic (15+) |
| `price_category` | Budget (<$5K), Mid-Range ($5K–$15K), Premium ($15K–$35K), High-End ($35K–$75K), Luxury ($75K+) |
| `mileage_category` | Low (<20K), Moderate (20K–60K), High (60K–100K), Very High (100K–150K), Extreme (150K+) |
| `price_per_mile` | price ÷ odometer; null when odometer = 0 or null |
| `depreciation_index` | price ÷ vehicle_age; null when age ≤ 0 |
| `posting_year/month/day/weekday/hour` | Extracted from posting_date timestamp |
| `posting_period` | Night (0–5h), Morning (6–11h), Afternoon (12–17h), Evening (18–23h) |
| `is_weekend` | True if posting_weekday is Saturday or Sunday |
| `is_luxury` | True if manufacturer in set of 17 luxury brands |
| `is_alternative_fuel` | True if fuel in {electric, hybrid, other} |
| `is_automatic` | True if transmission == "automatic" |
| `is_salvage` | True if title_status in {salvage, rebuilt} |
| `vehicle_segment` | Passenger Car / SUV+Off-Road / Truck / Van+Minivan / Wagon / Bus+Commercial |

**Output:** Parquet on HDFS + single CSV export (`coalesce(1)`) for Tableau.

### Hive Data Warehouse
The Featured dataset is registered as a Hive **External Table** (`vehicles_db.gold_vehicle`).

**Why External?** Hive stores only the schema metadata. The actual Parquet data remains in HDFS. Dropping the table removes only the catalog entry — the underlying data is never lost.

SQL validation queries are run via Spark SQL on the Hive table to cross-verify KPIs, aggregations, and distributions before connecting Tableau.

---

## 10. Streaming Pipeline — Detailed Design

### Producer (`01_Gdrive_To_Confluent_Producer.py`)

Built as a Databricks DLT pipeline using `@dlt.table` and `@dlt.append_flow` decorators.

**Step 1 — Auto Loader (Bronze DLT Table)**
```python
@dlt.table(name="gdrive_bronze_raw")
def gdrive_bronze_raw():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("multiLine", "true") \
        .option("mode", "PERMISSIVE") \
        .load(VOLUME_CSV_DIRECTORY)
```
Auto Loader tracks processed files — only new CSVs are ingested on each trigger.

**Step 2 — Confluent Kafka Sink**
```python
dlt.create_sink(
    name="confluent_kafka_sink",
    format="kafka",
    options={
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": jaas_config,
        "topic": TARGET_KAFKA_TOPIC
    }
)
```
SASL/SSL used because Confluent Cloud is publicly accessible — authentication + encryption in transit required.

**Step 3 — Append Flow (Data Push)**
```python
@dlt.append_flow(target="confluent_kafka_sink")
def confluent_producer_egress():
    df = spark.readStream.option("maxRowsPerTrigger", 1000) \
              .table("LIVE.gdrive_bronze_raw")
    return df.select(
        F.col("id").cast("string").alias("key"),
        F.to_json(F.struct("*")).alias("value")
    )
```
`maxRowsPerTrigger=1000` prevents bursting; key=vehicle id for Kafka partition routing.

### Consumer (`02_Confluent_To_Silver_Gold_Consumer.py`)

**Explicit Schema (Schema-on-Read)**
All 25 fields explicitly typed in a `StructType`. `inferSchema` does not work in streaming — Spark requires the schema upfront.

**Kafka Source Read**
```python
spark.readStream.format("kafka")
    .option("subscribe", SOURCE_KAFKA_TOPIC)
    .option("startingOffsets", "latest")  # only new data; historical = batch job
    .load()
```

**JSON Parsing Pipeline**
```
raw bytes → CAST(value AS STRING) → from_json(schema) → select("parsed_data.*")
```

**Silver Quality Features**

`price_quality_status` — log10-based price validation:
- `ZERO_PRICE` — price == 0
- `SUSPECT_LOW` — log10(price) < 2.7 (below ~$500)
- `SUSPECT_HIGH` — log10(price) > 5.2 (above ~$150,000)
- `VALID` — all others

Log scale chosen because price distribution is highly skewed — linear thresholds would be inadequate.

`listing_quality_score` (0–5) — counts presence of: VIN, description, latitude, paint_color, condition. Enables downstream filtering of high-quality listings.

**Gold Aggregation (Real-Time)**
```python
df_silver.groupBy("manufacturer", "state", "price_quality_status")
.agg(
    F.count("*").alias("total_active_listings"),
    F.avg(F.col("price").cast("double")).alias("average_market_price"),
    F.avg(F.col("odometer").cast("double")).alias("average_mileage"),
    F.max(F.col("posting_date")).alias("last_updated_timestamp")
)
```
Stateful streaming aggregation — Spark maintains running aggregates and updates them on each micro-batch.

---

## 11. Airflow Pipeline Orchestration

Apache Airflow orchestrates the complete batch pipeline lifecycle.

### DAG Structure
```
Bronze Ingest → Silver ETL → Feature Engineering → Hive Load → Kafka Production → Streaming Jobs
```

### Key DAG Properties

| Property | Value |
|---|---|
| Schedule | Daily (cron-based) |
| Retries | Configured with exponential backoff |
| Dependency enforcement | `Silver_job >> Gold_job` — Gold never runs if Silver fails |
| Alerts | Email / Slack on task failure or SLA breach |
| Task logging | Full logs accessible from Airflow UI |
| Environment management | Parameterised via Airflow Variables and Connections |

Airflow provides fault tolerance, observability, and reproducibility for the entire pipeline — ensuring each layer is processed in the correct order with clear failure recovery.

---

## 12. Data Validation & Quality Assurance

### Batch Pipeline Validation
- Row counts logged at each stage (Bronze → Silver → Gold)
- Silver schema `printSchema()` validated before writing to HDFS
- Hive SQL queries run post-registration to cross-verify KPI values against expected ranges
- Tableau dashboard KPIs cross-verified against Hive SQL aggregation outputs

### Streaming Pipeline Validation
- `listing_quality_score` distribution monitored per micro-batch
- `price_quality_status` breakdown logged to confirm VALID record ratio
- `id.isNotNull()` filter enforced before Silver table write
- Explicit `StructType` ensures malformed JSON records fail gracefully

---

## 13. Dashboard Results

### KPI Summary (from Tableau Executive Dashboard)

| KPI | Value |
|---|---|
| Total Listings | 2,42,979 |
| Average Market Price | $15,365 |
| Median Price | $10,999 |
| Average Mileage | 1,07,376 miles |
| Average Vehicle Age | 15.40 years |
| Total Manufacturers | 43 |

### Dashboard 1 — Executive Market Overview

<img width="1600" height="1025" alt="DashBoard_1" src="https://github.com/user-attachments/assets/0694a7ba-ad6f-4cdc-8bd5-7479e8e711f2" />

Provides a high-level summary of the used vehicle marketplace.

**Key Metrics:** Total Listings, Avg Price, Median Price, Avg Mileage, Avg Vehicle Age, Total Manufacturers

**Visualisations:**
- USA Choropleth Map — Avg Price by State (Alaska darkest — highest concentration)
- Listings by Month — April: 1,64,906 listings; May: 78,073 listings
- Top 10 Manufacturers by Listings — Ford leads at 40,650 (Chevrolet, Toyota, Honda follow)
- Fuel Distribution — Gas 88.2%, Diesel, Electric 0.3%, Hybrid, Other, Unknown
- Vehicle Condition Distribution — Unknown 96,433, Excellent 69,642, Good 52,284

**Interactivity:** Click any state on the map → all KPIs and charts filter to that state. Click any manufacturer → dashboard updates for that brand.

### Dashboard 2 — Price Intelligence Dashboard

<img width="1280" height="840" alt="Dashboard_2" src="https://github.com/user-attachments/assets/cc6d0e73-a50d-4709-b647-42f8f8c9033c" />


Focuses on pricing behaviour across different market segments and manufacturers.

**Key Metrics:** Avg Price $15,365 · Highest Price $150,000 · Lowest Price $100 · Median Price $10,999

**Visualisations:**
- Top 10 Manufacturers by Average Price — Ferrari leads at $87,808; Audi, Mercedes-Benz follow
- Average Price Comparison by Month — April avg $15,816; May avg $14,414
- Average Price by Fuel Type — Diesel $29,641 (highest); Hybrid $12,834 (lowest among standard fuels)
- Average Price by Vehicle Condition — New $26,829; Like New $18,452; Salvage $3,692
- Mileage vs Average Price Scatter — Luxury brands (orange) show higher avg price at all mileage levels
- Average Price by Manufacturer × Condition Heatmap — Ferrari new: ~$100K; Good condition cars across brands visible

**Engineered Features Used:** `price_per_mile`, `price_category`, `depreciation_index`, `is_luxury`

---

## 14. Project Outcomes

The platform successfully demonstrates:

- End-to-end Medallion Architecture implementation (Bronze → Silver → Featured → Gold)
- Distributed Spark ETL with advanced multi-strategy null imputation using Window functions
- Manufacturer inference via model-keyword broadcast lookup
- Hive External Data Warehouse with Parquet partitioning by state
- Parallel streaming pipeline on Databricks (DLT + Confluent Kafka + Spark Structured Streaming)
- Price quality scoring and listing completeness scoring in the streaming Silver layer
- Apache Airflow DAG orchestration with retries, SLAs, and dependency management
- Interactive Tableau dashboards: Executive Overview + Price Intelligence (242,979 listings, 50 states, 43 manufacturers)

---

## 15. Future Enhancements

| Enhancement | Description |
|---|---|
| AWS S3 + Delta Lake | Migrate HDFS storage to S3; adopt Delta format for ACID transactions and time-travel queries |
| ML Price Prediction | Train a regression model on engineered Gold features to predict fair market value |
| Apache Iceberg / Flink | Evaluate Iceberg for table format evolution; Flink for sub-second streaming latency |
| Docker + Kubernetes | Containerise Spark jobs and Airflow workers for reproducible, cloud-agnostic deployments |
| CI/CD for Pipelines | Automated testing, linting, and promotion workflows (dev → staging → prod) |
| Additional Dashboards | Vehicle Health & Inventory, Geographic Market Analytics, Time & Activity Analytics |
| Streaming Deduplication | `dropDuplicates("id")` with watermarking in Spark Structured Streaming |
| Real-time Dashboard Auto-refresh | Connect Tableau to Gold streaming table with live extract refresh |

---

## 16. End-to-End Pipeline Flow

```
Phase 0 — Dataset Profiling
  ├── Schema validation (26 columns, mixed types)
  ├── Missing value analysis (manufacturer, odometer, transmission most affected)
  ├── Duplicate analysis (fuzzy duplicates on mfr+model+year+price+odo)
  ├── Data type inspection (all read as STRING initially)
  └── Data quality report

  ↓

Phase 1 — Bronze Layer
  └── Raw CSV → HDFS (immutable, no modifications)

  ↓

Phase 2 — Silver Layer (demo-eda.py)
  ├── Type casting (lat, long, posting_date, price, year, odometer)
  ├── Categorical normalisation (lowercase + trim)
  ├── Manufacturer inference (model-keyword lookup, 70+ entries)
  ├── Odometer imputation (cascading window medians)
  ├── Transmission mode imputation (window frequency ranking)
  ├── State code enrichment (broadcast join, 51-row lookup)
  ├── Price & year filters (business rule enforcement)
  └── Output: Parquet on HDFS, partitioned by state_name

  ↓

Phase 3 — Feature Engineering / Gold Layer (demo-gold.py)
  ├── 13 derived features (vehicle_age, price_category, mileage_category,
  │   price_per_mile, depreciation_index, posting features, boolean flags,
  │   vehicle_segment)
  └── Output: Parquet on HDFS + single CSV (coalesce(1))

  ↓

Phase 4 — Hive Data Warehouse (03_Hive_Data_Warehouse.ipynb)
  ├── External table registration (vehicles_db.gold_vehicle)
  ├── SQL validation queries (KPIs, manufacturer splits, state distributions)
  └── CSV export for Tableau connectivity

  ↓

Phase 5 — Apache Airflow Orchestration
  ├── DAG: Silver job >> Gold job (dependency enforced)
  ├── Daily cron schedule
  ├── Retry with backoff, SLA alerts
  └── Full task logging

  ↓

Phase 6 — Kafka Producer (01_Gdrive_To_Confluent_Producer.py on Databricks)
  ├── Auto Loader reads incremental CSVs from Databricks Volume
  ├── Bronze DLT table: gdrive_bronze_raw
  └── append_flow → Confluent Kafka topic (SASL/SSL, key=id, value=JSON)

  ↓

Phase 7 — Spark Structured Streaming Consumer (02_Confluent_To_Silver_Gold_Consumer.py)
  ├── Kafka source read (startingOffsets: latest)
  ├── Schema-on-read via explicit StructType (25 fields)
  ├── Silver DLT: cleaning, price_quality_status, listing_quality_score
  └── Gold DLT: groupBy(manufacturer, state, price_quality_status) → KPI aggregations

  ↓

Phase 8 — Tableau Dashboards
  ├── Dashboard 1: Executive Market Overview
  │   (USA Map, Top Manufacturers, Fuel Distribution, Condition, Monthly Listings)
  └── Dashboard 2: Price Intelligence
      (Price by Manufacturer, Fuel, Condition, Mileage Scatter, Heatmap)
```

---

*Platform designed and implemented by Harshit Ranjan.*
*Stack: Apache Spark · Hadoop HDFS · Apache Hive · Databricks DLT · Confluent Cloud Kafka · Apache Airflow · Tableau*
