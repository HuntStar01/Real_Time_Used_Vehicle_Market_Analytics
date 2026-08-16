#!/usr/bin/python3

from datetime import datetime
from logging import log
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

CURRENT_YEAR = datetime.now().year + 1 

SILVER_PATH = "hdfs:///user/huntstar/project/silver/vehicles_clean"
GOLD_PATH = "hdfs:///user/huntstar/project/gold/vehicles_listing"
GOLD_CSV_EXPORT_PATH = "hdfs:///user/huntstar/project/gold/vehicles_listing_csv"

LUXURY_BRANDS = {
    "bmw", "mercedes-benz", "audi", "lexus", "cadillac", "lincoln",
    "porsche", "jaguar", "land rover", "infiniti", "acura", "volvo",
    "tesla", "maserati", "ferrari", "lamborghini", "genesis"
}

ALTERNATIVE_FUELS = {"electric", "hybrid", "other"}

# Features where nulls are expected due to missing source values
# These will not trigger a warning in the validation report
EXPECTED_NULL_FEATURES = {"price_per_mile", "depreciation_index"}

def run_batch_job():
    # 1. Initialize standalone Spark Session
    # 'local[*]' instructs Spark to use all available CPU cores on your machine
    # spark = SparkSession.builder \
    #     .appName("StandaloneBatchProcessing") \
    #     .master("local[*]") \
    #     .getOrCreate()

    spark = SparkSession.builder \
        .appName("VehicleSilver") \
        .enableHiveSupport() \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") 

    print("Spark Session initialized successfully.")

    try:
        # 2. Extract: Load your data (Simulating reading a CSV)
        # In practice, replace this with: spark.read.csv("path/to/input.csv", header=True, inferSchema=True)
        
        df = spark.read.parquet(SILVER_PATH)
        record_count_in = df.count()
        columns_in = len(df.columns)          # stored once, not re-read later
        print(f"  Records loaded : {record_count_in}")
        print(f"  Columns loaded : {columns_in}")

        df = df.withColumn(
            "vehicle_age",
            (F.lit(CURRENT_YEAR) - F.col("year")).cast(IntegerType())
        )

        df = df.withColumn(
            "age_group",
            F.when(F.col("vehicle_age") <= 2,  "Nearly New (0-2 yrs)")
             .when(F.col("vehicle_age") <= 5,  "Recent (3-5 yrs)")
             .when(F.col("vehicle_age") <= 10, "Mid-Age (6-10 yrs)")
             .when(F.col("vehicle_age") <= 15, "Older (11-15 yrs)")
             .otherwise("Classic (15+ yrs)")
        ) 

        df = df.withColumn(
            "price_category",
            F.when(F.col("price") < 5000,  "Budget (< $5K)")
             .when(F.col("price") < 15000, "Mid-Range ($5K-$15K)")
             .when(F.col("price") < 35000, "Premium ($15K-$35K)")
             .when(F.col("price") < 75000, "High-End ($35K-$75K)")
             .otherwise("Luxury ($75K+)")
        )

        df = df.withColumn(
            "mileage_category",
            F.when(F.col("odometer") < 20000,  "Low (< 20K)")
             .when(F.col("odometer") < 60000,  "Moderate (20K-60K)")
             .when(F.col("odometer") < 100000, "High (60K-100K)")
             .when(F.col("odometer") < 150000, "Very High (100K-150K)")
             .otherwise("Extreme (150K+)")
        )

        df = df.withColumn(
            "price_per_mile",
            F.when(
                F.col("odometer").isNotNull() & (F.col("odometer") > 0),
                F.round(F.col("price") / F.col("odometer"), 4)
            ).otherwise(None)
        )

        df = df \
            .withColumn("posting_year", F.year(F.col("posting_date"))) \
            .withColumn("posting_day", F.dayofmonth(F.col("posting_date"))) \
            .withColumn("posting_month", F.date_format(F.col("posting_date"), "MMMM")) \
            .withColumn("posting_weekday", F.date_format(F.col("posting_date"), "EEEE")) \
            .withColumn("posting_hour", F.hour(F.col("posting_date")))
        # silver_df.select(F.col("listing_quality_score")).distinct().show()


        df = df.withColumn(
            "is_weekend",
            F.when(F.col("posting_weekday").isin("Saturday", "Sunday"), True)
             .otherwise(False)
        )

        df = df.withColumn(
            "posting_period",
            F.when(F.col("posting_hour").between(0, 5),   "Night")
             .when(F.col("posting_hour").between(6, 11),  "Morning")
             .when(F.col("posting_hour").between(12, 17), "Afternoon")
             .otherwise("Evening")
        )

        df = df.withColumn(
            "is_luxury",
            F.when(F.col("manufacturer").isin(list(LUXURY_BRANDS)), True)
             .otherwise(False)
        )

        df = df.withColumn(
            "is_alternative_fuel",
            F.when(F.col("fuel").isin(list(ALTERNATIVE_FUELS)), True)
             .otherwise(False)
        )

        df = df.withColumn(
            "is_automatic",
            F.when(
                F.lower(F.col("transmission")) == "automatic",
                True
            ).otherwise(False)
        )

        df = df.withColumn(
            "vehicle_segment",
            F.when(F.col("type").isin("sedan", "coupe", "hatchback", "convertible"),
                   "Passenger Car")
             .when(F.col("type").isin("suv", "offroad"),
                   "SUV / Off-Road")
             .when(F.col("type").isin("truck", "pickup"),
                   "Truck")
             .when(F.col("type").isin("van", "mini-van"),
                   "Van / Minivan")
             .when(F.col("type").isin("wagon"),
                   "Wagon")
             .when(F.col("type").isin("bus"),
                   "Bus / Commercial")
             .otherwise("Other / Unknown")
        )


        df = df.withColumn(
            "is_salvage",
            F.when(F.col("title_status").isin("salvage", "rebuilt"), True)
             .otherwise(False)
        )

        df = df.withColumn(
            "depreciation_index",
            F.when(
                F.col("vehicle_age").isNull() |
                (F.col("vehicle_age") <= 0)  |
                F.col("price").isNull(),
                None
            ).otherwise(
                F.round(F.col("price") / F.col("vehicle_age"), 2)
            )
        )

        df = (
            df.withColumn("posting_dates", F.to_date(F.col("posting_date")))
              .withColumn("posting_time", F.date_format(F.col("posting_date"), "HH:mm:ss"))
        )
        df = df.drop("posting_date")

        print(df.printSchema())

        # df.write\
        #     .mode("overwrite")\
        #     .partitionBy("state_name")\
        #     .parquet(GOLD_PATH)

        # df.write \
        #     .mode("overwrite") \
        #     .option("compression", "gzip") \
        #     .partitionBy("state_name") \
        #     .option("path", GOLD_PATH) \
        #     .format("parquet") \
        #     .saveAsTable("vehicles_db.gold_vehicle")

        # df.coalesce(1) \
        #     .write \
        #     .mode("overwrite") \
        #     .option("header", "true") \
        #     .option("path", GOLD_PATH) \
        #     .format("csv")
            # .saveAsTable("vehicles_db.gold_vehicle")

        df.write \
            .mode("overwrite") \
            .format("parquet") \
            .save(GOLD_PATH)

        # df.coalesce(1) \
        #     .write \
        #     .mode("overwrite") \
        #     .option("header", "true") \
        #     .format("csv") \
        #     .save(GOLD_CSV_EXPORT_PATH)
        df.coalesce(1) \
            .write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("quoteAll", "true") \
            .format("csv") \
            .save(GOLD_CSV_EXPORT_PATH)
        
        print("New Row Count after transformations:", df.count())
        print("Data written to HDFS in Parquet format.")


        # print("Transformed Summary Data:")
        # transformed_df.show()

        # # 4. Load: Write the results out to disk
        # # This saves the data in efficient parquet format or csv
        # output_path = "output/category_revenue_summary"
        # transformed_df.write \
        #     .mode("overwrite") \
        #     .csv(output_path, header=True)
            
        # print(f"Batch processing complete. Results saved to: {output_path}")

    except Exception as e:
        print(f"An error occurred during the batch run: {e}")
        
    finally:
        # Always terminate the session to free up system memory
        spark.stop()
        print("Spark Session stopped.")

if __name__ == "__main__":
    run_batch_job()
