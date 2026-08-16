# =============================================================================
# Vehicle Market Pipeline — Bronze → Silver → Featured → Hive
# Craigslist Used Vehicle Dataset | CDAC Academic Project
# =============================================================================

from datetime import datetime
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

# =============================================================================
# CONFIGURATION
# =============================================================================

BRONZE_PATH   = "hdfs:///user/vehicle_market/bronze/vehicles.csv"
SILVER_PATH   = "hdfs:///user/vehicle_market/silver/vehicles_clean"
FEATURED_PATH = "hdfs:///user/vehicle_market/featured/vehicles_featured"
EXPORT_PATH   = "hdfs:///user/vehicle_market/exports/vehicles_featured_tableau"

HIVE_DATABASE = "vehicle_market"
HIVE_TABLE    = "vehicle_featured"

CURRENT_YEAR = datetime.now().year

# Columns where nulls are fine — filled with "unknown" instead of dropped
OPTIONAL_COLS = ["size", "condition", "cylinders", "drive", "paint_color", "type"]

# Columns to normalize (lowercase + trim whitespace)
CATEGORICAL_COLS = [
    "fuel", "condition", "drive", "transmission", "manufacturer",
    "title_status", "paint_color", "type", "size", "cylinders"
]

# Infer manufacturer from model name when manufacturer column is null
MANUFACTURER_LOOKUP = {
    # Ford
    "f-150": "ford", "f150": "ford", "mustang": "ford", "explorer": "ford",
    "escape": "ford", "focus": "ford", "fusion": "ford", "ranger": "ford",
    "expedition": "ford", "f-250": "ford", "f-350": "ford", "edge": "ford",
    # Chevrolet
    "silverado": "chevrolet", "camaro": "chevrolet", "malibu": "chevrolet",
    "tahoe": "chevrolet", "suburban": "chevrolet", "equinox": "chevrolet",
    "colorado": "chevrolet", "cruze": "chevrolet", "impala": "chevrolet",
    "blazer": "chevrolet", "traverse": "chevrolet",
    # Toyota
    "camry": "toyota", "corolla": "toyota", "rav4": "toyota",
    "tacoma": "toyota", "highlander": "toyota", "4runner": "toyota",
    "tundra": "toyota", "prius": "toyota", "sienna": "toyota",
    # Honda
    "civic": "honda", "accord": "honda", "cr-v": "honda",
    "pilot": "honda", "odyssey": "honda", "fit": "honda", "hrv": "honda",
    # Nissan
    "altima": "nissan", "sentra": "nissan", "maxima": "nissan",
    "rogue": "nissan", "frontier": "nissan", "pathfinder": "nissan", "murano": "nissan",
    # Dodge
    "ram": "dodge", "charger": "dodge", "challenger": "dodge",
    "durango": "dodge", "dart": "dodge", "caravan": "dodge",
    # Jeep
    "wrangler": "jeep", "cherokee": "jeep", "grand cherokee": "jeep",
    "compass": "jeep", "renegade": "jeep",
    # BMW
    "3 series": "bmw", "5 series": "bmw", "x5": "bmw", "x3": "bmw", "x1": "bmw",
    # Mercedes
    "c-class": "mercedes-benz", "e-class": "mercedes-benz", "glc": "mercedes-benz",
    "gle": "mercedes-benz",
    # Hyundai
    "elantra": "hyundai", "sonata": "hyundai", "tucson": "hyundai",
    "santa fe": "hyundai", "accent": "hyundai",
    # GMC
    "sierra": "gmc", "yukon": "gmc", "terrain": "gmc", "acadia": "gmc",
    # Subaru
    "outback": "subaru", "forester": "subaru", "impreza": "subaru",
    "crosstrek": "subaru", "legacy": "subaru",
    # Kia
    "sorento": "kia", "sportage": "kia", "optima": "kia", "soul": "kia",
    # Volkswagen
    "jetta": "volkswagen", "passat": "volkswagen", "tiguan": "volkswagen",
    "golf": "volkswagen", "atlas": "volkswagen",
}

# State code → full name
STATE_LOOKUP = [
    ("AL", "Alabama"),       ("AK", "Alaska"),        ("AZ", "Arizona"),
    ("AR", "Arkansas"),      ("CA", "California"),     ("CO", "Colorado"),
    ("CT", "Connecticut"),   ("DE", "Delaware"),       ("DC", "District of Columbia"),
    ("FL", "Florida"),       ("GA", "Georgia"),        ("HI", "Hawaii"),
    ("ID", "Idaho"),         ("IL", "Illinois"),       ("IN", "Indiana"),
    ("IA", "Iowa"),          ("KS", "Kansas"),         ("KY", "Kentucky"),
    ("LA", "Louisiana"),     ("ME", "Maine"),          ("MD", "Maryland"),
    ("MA", "Massachusetts"), ("MI", "Michigan"),       ("MN", "Minnesota"),
    ("MS", "Mississippi"),   ("MO", "Missouri"),       ("MT", "Montana"),
    ("NE", "Nebraska"),      ("NV", "Nevada"),         ("NH", "New Hampshire"),
    ("NJ", "New Jersey"),    ("NM", "New Mexico"),     ("NY", "New York"),
    ("NC", "North Carolina"),("ND", "North Dakota"),   ("OH", "Ohio"),
    ("OK", "Oklahoma"),      ("OR", "Oregon"),         ("PA", "Pennsylvania"),
    ("RI", "Rhode Island"),  ("SC", "South Carolina"), ("SD", "South Dakota"),
    ("TN", "Tennessee"),     ("TX", "Texas"),          ("UT", "Utah"),
    ("VT", "Vermont"),       ("VA", "Virginia"),       ("WA", "Washington"),
    ("WV", "West Virginia"), ("WI", "Wisconsin"),      ("WY", "Wyoming"),
]

LUXURY_BRANDS    = {"bmw", "mercedes-benz", "audi", "lexus", "cadillac", "lincoln",
                    "porsche", "jaguar", "land rover", "infiniti", "acura", "volvo",
                    "tesla", "maserati", "ferrari", "lamborghini", "genesis"}

ALTERNATIVE_FUELS = {"electric", "hybrid", "other"}

# =============================================================================
# SPARK SESSION
# =============================================================================

spark = SparkSession.builder \
    .appName("VehicleMarket_Pipeline") \
    .enableHiveSupport() \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# =============================================================================
# PHASE 1 — BRONZE → SILVER  (Data Cleaning)
# =============================================================================

print("\n" + "=" * 60)
print("  Phase 1 — Bronze → Silver (Data Cleaning)")
print("=" * 60)

# ── 1. Load Raw CSV ───────────────────────────────────────────────────────────

df = spark.read \
    .option("header",    "true") \
    .option("inferSchema", "false") \
    .option("multiLine", "true") \
    .option("quote",     '"') \
    .option("escape",    '"') \
    .csv(BRONZE_PATH)

print(f"Bronze loaded — {df.count()} rows, {len(df.columns)} columns")

# ── 2. Drop Columns with Too Many Nulls / No Analytical Value ─────────────────

df = df.drop("county")
print("Dropped: county")

# ── 3. Normalize Categorical Columns (lowercase + trim) ──────────────────────

for col in CATEGORICAL_COLS:
    if col in df.columns:
        df = df.withColumn(col, F.lower(F.trim(F.col(col))))

# ── 4. Fill Optional Columns — null → "unknown" ───────────────────────────────

for col in OPTIONAL_COLS:
    df = df.withColumn(
        col,
        F.when(F.col(col).isNull() | (F.trim(F.col(col)) == ""), "unknown")
         .otherwise(F.col(col))
    )

# ── 5. Infer Missing Manufacturer from Model Name ─────────────────────────────
#       e.g. model = "f-150 xlt" → manufacturer = "ford"

df = df.withColumn("model_lower", F.lower(F.trim(F.col("model"))))

inferred = F.lit(None).cast("string")
for model_kw, mfr in MANUFACTURER_LOOKUP.items():
    inferred = F.when(F.col("model_lower").contains(model_kw), F.lit(mfr)).otherwise(inferred)

df = df.withColumn(
    "manufacturer",
    F.when(F.col("manufacturer").isNull() | (F.col("manufacturer") == ""), inferred)
     .otherwise(F.col("manufacturer"))
).withColumn(
    "manufacturer",
    F.when(F.col("manufacturer").isNull(), "unknown").otherwise(F.col("manufacturer"))
).drop("model_lower")

# ── 6. Clean Model Column ─────────────────────────────────────────────────────

df = df.withColumn(
    "model",
    F.when(F.col("model").isNull() | (F.trim(F.col("model")) == ""), "unknown model")
     .otherwise(F.lower(F.trim(F.col("model"))))
)

# ── 7. Drop Rows Where Year is Null (required for age calculation later) ──────

before = df.count()
df = df.filter(F.col("year").isNotNull() & (F.trim(F.col("year")) != ""))
print(f"Dropped {before - df.count()} rows with null year")

# ── 8. Fill Remaining Business-Critical Nulls ─────────────────────────────────

for col in ["fuel", "title_status", "state"]:
    df = df.withColumn(
        col,
        F.when(F.col(col).isNull() | (F.trim(F.col(col)) == ""), "unknown")
         .otherwise(F.col(col))
    )

# ── 9. Add Full State Name (2-letter code → full name) ────────────────────────

state_df = spark.createDataFrame(STATE_LOOKUP, ["state_code", "state_name"])
df = df.withColumn("state", F.upper(F.col("state")))
df = df.join(F.broadcast(state_df), df.state == state_df.state_code, "left").drop("state_code")

# ── 10. Type Conversions ──────────────────────────────────────────────────────

df = df \
    .withColumn("price",        F.col("price").cast(FloatType())) \
    .withColumn("odometer",     F.col("odometer").cast(FloatType())) \
    .withColumn("year",         F.col("year").cast(IntegerType())) \
    .withColumn("lat",          F.col("lat").cast(FloatType())) \
    .withColumn("long",         F.col("long").cast(FloatType())) \
    .withColumn("posting_date", F.to_timestamp(F.col("posting_date")))

# ── 11. Odometer Imputation — Model → Manufacturer → Global median ────────────
#       Cascade: try the narrowest group first, fall back to broader ones

global_median = (
    df.filter(F.col("odometer").isNotNull())
      .select(F.percentile_approx("odometer", 0.5))
      .first()[0]
)

w_model = Window.partitionBy("manufacturer", "model")
w_manuf = Window.partitionBy("manufacturer")

df = df.withColumn(
    "odometer",
    F.coalesce(
        F.col("odometer"),
        F.percentile_approx("odometer", 0.5).over(w_model),
        F.percentile_approx("odometer", 0.5).over(w_manuf),
        F.lit(global_median)
    )
)

# ── 12. Transmission Imputation — most common value per Manufacturer + Model ──

# Step 1: Count how often each transmission value appears per group
trans_counts = (
    df.filter(F.col("transmission").isNotNull())
      .groupBy("manufacturer", "model", "transmission")
      .agg(F.count("*").alias("cnt"))
)

# Step 2: Pick the most frequent transmission per group (row_number = 1)
w_rank = Window.partitionBy("manufacturer", "model").orderBy(F.desc("cnt"), "transmission")

trans_mode = (
    trans_counts
    .withColumn("rn", F.row_number().over(w_rank))
    .filter(F.col("rn") == 1)
    .select("manufacturer", "model", F.col("transmission").alias("mode_transmission"))
)

# Step 3: Fill nulls — group mode → global fallback "automatic"
df = df.join(trans_mode, on=["manufacturer", "model"], how="left")
df = df.withColumn(
    "transmission",
    F.coalesce(F.col("transmission"), F.col("mode_transmission"), F.lit("automatic"))
).drop("mode_transmission")

# ── 13. Remove Duplicates ─────────────────────────────────────────────────────

before = df.count()

df = df.dropDuplicates(["id"])

# VIN-based dedup (only for rows that have a VIN)
df_with_vin    = df.filter(F.col("VIN").isNotNull()).dropDuplicates(["VIN"])
df_without_vin = df.filter(F.col("VIN").isNull())
df = df_with_vin.union(df_without_vin)

df = df.dropDuplicates(["url"])

print(f"Removed {before - df.count()} duplicate rows")

# ── 14. Business Rule Filters ─────────────────────────────────────────────────

before = df.count()
df = df.filter(F.col("price").isNotNull() & (F.col("price") > 0))
df = df.filter(F.col("year").between(1980, CURRENT_YEAR + 1))
print(f"Removed {before - df.count()} rows failing business rules (price > 0, year 1980–present)")

# ── 15. Save Silver Layer ─────────────────────────────────────────────────────

df.write.mode("overwrite").parquet(SILVER_PATH)
print(f"Silver layer saved — {df.count()} rows, {len(df.columns)} columns")

# =============================================================================
# PHASE 2 — SILVER → FEATURED  (Feature Engineering)
# =============================================================================

print("\n" + "=" * 60)
print("  Phase 2 — Feature Engineering")
print("=" * 60)

df = spark.read.parquet(SILVER_PATH)
print(f"Silver loaded — {df.count()} rows, {len(df.columns)} columns")

# ── Vehicle Age & Age Group ───────────────────────────────────────────────────

df = df.withColumn("vehicle_age", (F.lit(CURRENT_YEAR) - F.col("year")).cast(IntegerType()))

df = df.withColumn(
    "age_group",
    F.when(F.col("vehicle_age") <= 2,  "Nearly New (0-2 yrs)")
     .when(F.col("vehicle_age") <= 5,  "Recent (3-5 yrs)")
     .when(F.col("vehicle_age") <= 10, "Mid-Age (6-10 yrs)")
     .when(F.col("vehicle_age") <= 15, "Older (11-15 yrs)")
     .otherwise("Classic (15+ yrs)")
)

# ── Price Category ────────────────────────────────────────────────────────────

df = df.withColumn(
    "price_category",
    F.when(F.col("price") < 5000,  "Budget (< $5K)")
     .when(F.col("price") < 15000, "Mid-Range ($5K-$15K)")
     .when(F.col("price") < 35000, "Premium ($15K-$35K)")
     .when(F.col("price") < 75000, "High-End ($35K-$75K)")
     .otherwise("Luxury ($75K+)")
)

# ── Mileage Category ──────────────────────────────────────────────────────────

df = df.withColumn(
    "mileage_category",
    F.when(F.col("odometer") < 20000,  "Low (< 20K)")
     .when(F.col("odometer") < 60000,  "Moderate (20K-60K)")
     .when(F.col("odometer") < 100000, "High (60K-100K)")
     .when(F.col("odometer") < 150000, "Very High (100K-150K)")
     .otherwise("Extreme (150K+)")
)

# ── Price per Mile ────────────────────────────────────────────────────────────
#   Null when odometer is 0 (avoids divide-by-zero)

df = df.withColumn(
    "price_per_mile",
    F.when(F.col("odometer") > 0, F.round(F.col("price") / F.col("odometer"), 4))
     .otherwise(None)
)

# ── Posting Date Features ─────────────────────────────────────────────────────

df = df \
    .withColumn("posting_year",    F.year("posting_date")) \
    .withColumn("posting_month",   F.date_format("posting_date", "MMMM")) \
    .withColumn("posting_day",     F.dayofmonth("posting_date")) \
    .withColumn("posting_weekday", F.date_format("posting_date", "EEEE")) \
    .withColumn("posting_hour",    F.hour("posting_date"))

df = df.withColumn("is_weekend", F.col("posting_weekday").isin("Saturday", "Sunday"))

df = df.withColumn(
    "posting_period",
    F.when(F.col("posting_hour").between(0, 5),   "Night")
     .when(F.col("posting_hour").between(6, 11),  "Morning")
     .when(F.col("posting_hour").between(12, 17), "Afternoon")
     .otherwise("Evening")
).drop("posting_hour")

# ── Vehicle Flags ─────────────────────────────────────────────────────────────

df = df.withColumn("is_luxury",           F.col("manufacturer").isin(list(LUXURY_BRANDS)))
df = df.withColumn("is_alternative_fuel", F.col("fuel").isin(list(ALTERNATIVE_FUELS)))
df = df.withColumn("is_salvage",          F.col("title_status").isin("salvage", "rebuilt"))

# ── Vehicle Segment ───────────────────────────────────────────────────────────

df = df.withColumn(
    "vehicle_segment",
    F.when(F.col("type").isin("sedan", "coupe", "hatchback", "convertible"), "Passenger Car")
     .when(F.col("type").isin("suv", "offroad"),                             "SUV / Off-Road")
     .when(F.col("type").isin("truck", "pickup"),                            "Truck")
     .when(F.col("type").isin("van", "mini-van"),                            "Van / Minivan")
     .when(F.col("type") == "wagon",                                         "Wagon")
     .when(F.col("type") == "bus",                                           "Bus / Commercial")
     .otherwise("Other / Unknown")
)

# ── Depreciation Index  =  Price / Vehicle Age ────────────────────────────────
#   Higher value = more value retained per year (newer/expensive vehicles)
#   Null when vehicle_age = 0 (brand-new — avoids divide-by-zero)

df = df.withColumn(
    "depreciation_index",
    F.when(F.col("vehicle_age") > 0, F.round(F.col("price") / F.col("vehicle_age"), 2))
     .otherwise(None)
)

# ── Split posting_date → posting_dates (date) + posting_time (time string) ────

df = df \
    .withColumn("posting_dates", F.to_date("posting_date")) \
    .withColumn("posting_time",  F.date_format("posting_date", "HH:mm:ss")) \
    .drop("posting_date")

# ── Save Featured Layer ───────────────────────────────────────────────────────

df.write.mode("overwrite").parquet(FEATURED_PATH)
print(f"Featured layer saved — {df.count()} rows, {len(df.columns)} columns")

# ── Sample Distributions ──────────────────────────────────────────────────────

print("\nAge Group Distribution:")
df.groupBy("age_group").count().orderBy("age_group").show(truncate=False)

print("Price Category Distribution:")
df.groupBy("price_category").count().orderBy(F.desc("count")).show(truncate=False)

print("Vehicle Segment Distribution:")
df.groupBy("vehicle_segment").count().orderBy(F.desc("count")).show(truncate=False)

# =============================================================================
# PHASE 3 — HIVE DATA WAREHOUSE
# =============================================================================

print("\n" + "=" * 60)
print("  Phase 3 — Hive Data Warehouse")
print("=" * 60)

df = spark.read.parquet(FEATURED_PATH)
print(f"Featured dataset loaded — {df.count()} rows")

# ── Create Database & Table ───────────────────────────────────────────────────

spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
spark.sql(f"USE {HIVE_DATABASE}")
spark.sql(f"DROP TABLE IF EXISTS {HIVE_DATABASE}.{HIVE_TABLE}")

spark.sql(f"""
    CREATE EXTERNAL TABLE {HIVE_DATABASE}.{HIVE_TABLE} (

        -- Original Cleaned Columns (Phase 1)
        id              STRING,
        url             STRING,
        region          STRING,
        price           FLOAT,
        year            INT,
        manufacturer    STRING,
        model           STRING,
        `condition`     STRING,
        cylinders       STRING,
        fuel            STRING,
        odometer        FLOAT,
        title_status    STRING,
        transmission    STRING,
        drive           STRING,
        size            STRING,
        `type`          STRING,
        paint_color     STRING,
        state           STRING,
        state_name      STRING,
        lat             FLOAT,
        long            FLOAT,
        VIN             STRING,

        -- Engineered Features (Phase 2)
        vehicle_age         INT,
        age_group           STRING,
        price_category      STRING,
        price_per_mile      DOUBLE,
        depreciation_index  DOUBLE,
        mileage_category    STRING,
        posting_year        INT,
        posting_month       STRING,
        posting_day         INT,
        posting_weekday     STRING,
        is_weekend          BOOLEAN,
        posting_period      STRING,
        posting_dates       DATE,
        posting_time        STRING,
        is_luxury           BOOLEAN,
        is_alternative_fuel BOOLEAN,
        is_salvage          BOOLEAN,
        vehicle_segment     STRING

    )
    STORED AS PARQUET
    LOCATION '{FEATURED_PATH}'
""")

print(f"Hive table '{HIVE_DATABASE}.{HIVE_TABLE}' created.")
spark.sql(f"SELECT COUNT(*) AS total_records FROM {HIVE_DATABASE}.{HIVE_TABLE}").show()
spark.sql(f"SELECT * FROM {HIVE_DATABASE}.{HIVE_TABLE} LIMIT 5").show(truncate=False)

# ── Analytical Queries ────────────────────────────────────────────────────────

print("\n-- Top 10 Manufacturers by Listing Count --")
spark.sql(f"""
    SELECT manufacturer,
           COUNT(*)              AS listings,
           ROUND(AVG(price), 0)  AS avg_price
    FROM {HIVE_DATABASE}.{HIVE_TABLE}
    GROUP BY manufacturer
    ORDER BY listings DESC
    LIMIT 10
""").show(truncate=False)

print("\n-- Fuel Type Distribution --")
spark.sql(f"""
    SELECT fuel,
           COUNT(*) AS listings,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {HIVE_DATABASE}.{HIVE_TABLE}
    GROUP BY fuel
    ORDER BY listings DESC
""").show(truncate=False)

print("\n-- Price Stats (min / avg / max) --")
spark.sql(f"""
    SELECT ROUND(AVG(price), 0) AS avg_price,
           MIN(price)           AS min_price,
           MAX(price)           AS max_price
    FROM {HIVE_DATABASE}.{HIVE_TABLE}
""").show()

print("\n-- Top 10 States by Listing Count --")
spark.sql(f"""
    SELECT state_name,
           COUNT(*) AS listings
    FROM {HIVE_DATABASE}.{HIVE_TABLE}
    GROUP BY state_name
    ORDER BY listings DESC
    LIMIT 10
""").show(truncate=False)

print("\n-- Vehicle Condition Distribution --")
spark.sql(f"""
    SELECT `condition`,
           COUNT(*) AS listings,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {HIVE_DATABASE}.{HIVE_TABLE}
    GROUP BY `condition`
    ORDER BY listings DESC
""").show(truncate=False)

print("\n-- Transmission Distribution --")
spark.sql(f"""
    SELECT transmission,
           COUNT(*) AS listings,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {HIVE_DATABASE}.{HIVE_TABLE}
    GROUP BY transmission
    ORDER BY listings DESC
""").show(truncate=False)

print("\n-- Avg Depreciation Index by Manufacturer (Top 10) --")
spark.sql(f"""
    SELECT manufacturer,
           ROUND(AVG(depreciation_index), 2) AS avg_depreciation,
           COUNT(*)                          AS listings
    FROM {HIVE_DATABASE}.{HIVE_TABLE}
    WHERE depreciation_index IS NOT NULL
    GROUP BY manufacturer
    ORDER BY avg_depreciation DESC
    LIMIT 10
""").show(truncate=False)

print("\n-- Weekend vs Weekday Listing Split --")
spark.sql(f"""
    SELECT is_weekend,
           COUNT(*) AS listings,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {HIVE_DATABASE}.{HIVE_TABLE}
    GROUP BY is_weekend
    ORDER BY is_weekend
""").show(truncate=False)

print("\n-- Posting Period Distribution --")
spark.sql(f"""
    SELECT posting_period,
           COUNT(*) AS listings,
           ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
    FROM {HIVE_DATABASE}.{HIVE_TABLE}
    GROUP BY posting_period
    ORDER BY listings DESC
""").show(truncate=False)

# ── Export Single CSV for Tableau ─────────────────────────────────────────────

print("\nExporting single CSV for Tableau...")

spark.sql(f"SELECT * FROM {HIVE_DATABASE}.{HIVE_TABLE}") \
    .coalesce(1) \
    .write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(EXPORT_PATH)

print(f"CSV exported to: {EXPORT_PATH}")

# ── Done ──────────────────────────────────────────────────────────────────────

print("\n" + "=" * 60)
print("  Pipeline complete — Bronze → Silver → Featured → Hive")
print("=" * 60)

spark.stop()
