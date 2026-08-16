# Databricks notebook source
import dlt
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# --- 1. CONFIGURATION PARAMETERS ---
VOLUME_CSV_DIRECTORY = "/Volumes/workspace/csv/big-data-project/"
CONFLUENT_BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
CONFLUENT_API_KEY = "JXZAQZZHLFGGMBVY"
CONFLUENT_API_SECRET = "cfltVaCnR7UcM6SsBCka5Ryj2miE2TnHBIhE/dTJ9/6Goq+5nJppgFjhViDhqy/Q"
SOURCE_KAFKA_TOPIC = "gdrive-csv-stream-topic"

# --- 2. DEFINE EXPLICIT TARGET SCHEMA-ON-READ ---
csv_file_schema = StructType([
    StructField("id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("region", StringType(), True),
    StructField("region_url", StringType(), True),
    StructField("price", StringType(), True),
    StructField("year", StringType(), True),
    StructField("manufacturer", StringType(), True),
    StructField("model", StringType(), True),
    StructField("condition", StringType(), True),
    StructField("cylinders", StringType(), True),
    StructField("fuel", StringType(), True),
    StructField("odometer", StringType(), True),
    StructField("title_status", StringType(), True),
    StructField("transmission", StringType(), True),
    StructField("VIN", StringType(), True),
    StructField("drive", StringType(), True),
    StructField("size", StringType(), True),
    StructField("type", StringType(), True),
    StructField("paint_color", StringType(), True),
    StructField("image_url", StringType(), True),
    StructField("description", StringType(), True),
    StructField("state", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("long", DoubleType(), True),
    StructField("posting_date", TimestampType(), True)
])

# --- 3. SILVER LAYER: REAL-TIME DATA CLEANING & SCHEMA MATCHING ---
@dlt.table(
    name="vehicles_confluent_silver_dlt",
    comment="Consumes JSON stream from Confluent Cloud, mapping structural parameters exactly to target schemas."
)
def transform_kafka_stream():
    jaas_config = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="' + CONFLUENT_API_KEY + '" password="' + CONFLUENT_API_SECRET + '";'

    raw_kafka_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", CONFLUENT_BOOTSTRAP_SERVER)
        .option("kafka.sasl.mechanism", "PLAIN")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.jaas.config", jaas_config)
        .option("subscribe", SOURCE_KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )
    
    df = (
        raw_kafka_stream
        .selectExpr("CAST(value AS STRING) as json_string_payload")
        .select(F.from_json(F.col("json_string_payload"), csv_file_schema).alias("parsed_data"))
        .select("parsed_data.*")
    )
    
    # Strictly cast double and timestamp properties to align with target schema structural constraints
    df = df.withColumn("lat", F.col("lat").cast("double")) \
           .withColumn("long", F.col("long").cast("double")) \
           .withColumn("posting_date", F.col("posting_date").cast("timestamp"))

    fill_values = {
        "id": "unknown", "url": "unknown", "region": "unknown", "region_url": "unknown",
        "price": "0", "year": "unknown", "type": "unknown", "size": "unknown", "drive": "unknown",
        "cylinders": "unknown", "manufacturer": "unknown", "fuel": "unknown",
        "title_status": "unknown", "transmission": "unknown", "model": "unknown",
        "state": "unknown"
    }
    df_filled = df.fillna(fill_values)

    # Standardize string fields
    df_final = df_filled.withColumn("transmission", F.coalesce(F.col("transmission"), F.lit("automatic")))
    df_final = df_final.fillna({"VIN": "Not Available", "image_url": "No Image", "description": "No Description"})

    # Inject analytical parameters as top-level fields
    df_final = (df_final
        .withColumn("year_posted", F.year("posting_date"))
        .withColumn("month_posted", F.month("posting_date"))
    )

    # Enforce evaluation status strings based on logic rules
    silver_df = df_final.withColumn(
        "price_quality_status",
        F.when(F.col("price").cast("bigint") == 0, "ZERO_PRICE")
        .when(F.log10(F.col("price").cast("bigint")) < 2.7, "SUSPECT_LOW")
        .when(F.log10(F.col("price").cast("bigint")) > 5.2, "SUSPECT_HIGH")
        .otherwise("VALID")
    )

    silver_df = silver_df.withColumn(
        "listing_quality_score",
        F.expr("""
            (CASE WHEN VIN IS NOT NULL AND VIN<>'Not Available' AND VIN<>'' THEN 1 ELSE 0 END)+
            (CASE WHEN description IS NOT NULL AND description<>'No Description' AND description<>'' THEN 1 ELSE 0 END)+
            (CASE WHEN lat IS NOT NULL THEN 1 ELSE 0 END)+
            (CASE WHEN paint_color IS NOT NULL AND paint_color<>'unknown' AND paint_color<>'' THEN 1 ELSE 0 END)+
            (CASE WHEN condition IS NOT NULL AND condition<>'unknown' AND condition<>'' THEN 1 ELSE 0 END)
        """)
    )
    
    return silver_df.filter(F.col("id").isNotNull())

# --- 5. GOLD LAYER: REAL-TIME ANALYTICS STREAMING AGGREGATIONS ---
@dlt.table(
    name="vehicles_gold_analytics",
    comment="Real-time Gold analytics table calculating vehicle market KPIs continuously."
)
def vehicles_gold():
    df_silver = dlt.read_stream("vehicles_confluent_silver_dlt")
    return (
        df_silver
        .groupBy("manufacturer", "state", "price_quality_status")
        .agg(
            F.count("*").alias("total_active_listings"),
            F.avg(F.col("price").cast("double")).alias("average_market_price"),
            F.avg(F.col("odometer").cast("double")).alias("average_mileage"),
            F.max(F.col("posting_date")).alias("last_updated_timestamp")
        )
    )
