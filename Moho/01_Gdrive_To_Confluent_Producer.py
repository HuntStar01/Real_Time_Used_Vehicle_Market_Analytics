# Databricks notebook source
import dlt
import pyspark.sql.functions as F

# --- 1. CONFIGURATION PARAMETERS ---
VOLUME_CSV_DIRECTORY = "/Volumes/workspace/csv/big-data-project/"

CONFLUENT_BOOTSTRAP_SERVER = "pkc-921jm.us-east-2.aws.confluent.cloud:9092"
CONFLUENT_API_KEY = "JXZAQZZHLFGGMBVY"
CONFLUENT_API_SECRET = "cfltVaCnR7UcM6SsBCka5Ryj2miE2TnHBIhE/dTJ9/6Goq+5nJppgFjhViDhqy/Q"
TARGET_KAFKA_TOPIC = "gdrive-csv-stream-topic"

# --- 2. BRONZE LAYER: NATIVE SERVERLESS AUTO LOADER FROM VOLUME ---
@dlt.table(
    name="gdrive_bronze_raw",
    comment="Reads unclean multiline records properly from Volume storage using Auto Loader."
)
def gdrive_bronze_raw():
    return (
        spark.readStream
        .format("cloudFiles")                                     
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "false") 
        .option("multiLine", "true")    
        .option("quote", '"')           
        .option("escape", '"')          
        .option("mode", "PERMISSIVE")   
        .load(VOLUME_CSV_DIRECTORY)                             
    )

# --- 3. CONFLUENT KAFKA SINK AND FLOW DEFINITION ---
jaas_config = 'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="' + CONFLUENT_API_KEY + '" password="' + CONFLUENT_API_SECRET + '";'

# Define the external Kafka Target Sink using the DLT Sinks API
dlt.create_sink(
    name="confluent_kafka_sink",
    format="kafka",
    options={
        "kafka.bootstrap.servers": CONFLUENT_BOOTSTRAP_SERVER,
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.jaas.config": jaas_config,
        "topic": TARGET_KAFKA_TOPIC
    }
)

# Enforce an append flow to stream into the external target sink object
@dlt.append_flow(
    target="confluent_kafka_sink"
)
def confluent_producer_egress():
    # FIXED: Configured the 10-second processing window using readStream options
    df = (
        spark.readStream
        .option("maxRowsPerTrigger", 1000) 
        .option("processingTime", "10 seconds") # Replaces the invalid .trigger() dataframe call
        .table("LIVE.gdrive_bronze_raw")   
    )
    
    # Map fields to the required 'key' and 'value' parameters expected by Kafka sinks
    kafka_payload_df = df.select(
        F.col("id").cast("string").alias("key"),
        F.to_json(F.struct("*")).alias("value")
    )
    
    # Return the clean streaming DataFrame object directly as required by append_flow
    return kafka_payload_df
