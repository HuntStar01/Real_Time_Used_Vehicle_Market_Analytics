#!/usr/bin/python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, expr
import pyspark.sql.functions as F
from pyspark.sql import Window
from pyspark.sql.functions import upper, broadcast
from datetime import datetime


BRONZE_PATH = "hdfs:///user/mohitkumbhar/project/bronze/vehicles.csv"
SILVER_PATH = "hdfs:///user/mohitkumbhar/project/silver/vehicles_clean"

CURRENT_YEAR = datetime.now().year + 1

CATEGORY_B_COLS = ["size", "condition", "cylinders", "drive", "paint_color", "type"]

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

CATEGORICAL_NORMALIZE_COLS = [
    "fuel", "condition", "drive", "transmission", "manufacturer",
    "title_status", "paint_color", "type", "size", "cylinders"
]

state_lookup = [
    ("AL", "Alabama"),
    ("AK", "Alaska"),
    ("AZ", "Arizona"),
    ("AR", "Arkansas"),
    ("CA", "California"),
    ("CO", "Colorado"),
    ("CT", "Connecticut"),
    ("DE", "Delaware"),
    ("DC", "District of Columbia"),
    ("FL", "Florida"),
    ("GA", "Georgia"),
    ("HI", "Hawaii"),
    ("ID", "Idaho"),
    ("IL", "Illinois"),
    ("IN", "Indiana"),
    ("IA", "Iowa"),
    ("KS", "Kansas"),
    ("KY", "Kentucky"),
    ("LA", "Louisiana"),
    ("ME", "Maine"),
    ("MD", "Maryland"),
    ("MA", "Massachusetts"),
    ("MI", "Michigan"),
    ("MN", "Minnesota"),
    ("MS", "Mississippi"),
    ("MO", "Missouri"),
    ("MT", "Montana"),
    ("NE", "Nebraska"),
    ("NV", "Nevada"),
    ("NH", "New Hampshire"),
    ("NJ", "New Jersey"),
    ("NM", "New Mexico"),
    ("NY", "New York"),
    ("NC", "North Carolina"),
    ("ND", "North Dakota"),
    ("OH", "Ohio"),
    ("OK", "Oklahoma"),
    ("OR", "Oregon"),
    ("PA", "Pennsylvania"),
    ("RI", "Rhode Island"),
    ("SC", "South Carolina"),
    ("SD", "South Dakota"),
    ("TN", "Tennessee"),
    ("TX", "Texas"),
    ("UT", "Utah"),
    ("VT", "Vermont"),
    ("VA", "Virginia"),
    ("WA", "Washington"),
    ("WV", "West Virginia"),
    ("WI", "Wisconsin"),
    ("WY", "Wyoming")
]


def run_batch_job():
    # 1. Initialize standalone Spark Session

    spark = SparkSession.builder \
        .appName("Vehicle-Analysis") \
        .enableHiveSupport() \
        .config("spark.sql.debug.maxToStringFields", "1000") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN") 

    print("Spark Session initialized successfully.")

    try:
        # 2. Extract: Load your data (Simulating reading a CSV)
        # In practice, replace this with: spark.read.csv("path/to/input.csv", header=True, inferSchema=True)
        
        df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "false") \
                .option("multiLine", "true") \
                .option("quote", '"') \
                .option("escape", '"') \
                .option("mode", "PERMISSIVE") \
                .csv(BRONZE_PATH)
        
        print("Row Count:")
        print(df.count())

        # 3. Transform: Calculate Total Sales and filter 
        # Adding a calculated column and aggregating data by category
        df = df.withColumn("lat", F.col("lat").cast("double"))
        df = df.withColumn("long", F.col("long").cast("double"))
        df = df.withColumn("posting_date", F.col("posting_date").cast("timestamp"))

        df = df.drop("county")

        # Normalizing categorical columns
        for col in CATEGORICAL_NORMALIZE_COLS:
            if col in df.columns:
                df = df.withColumn(col, F.lower(F.trim(F.col(col))))

        for col in CATEGORY_B_COLS:
            df = df.withColumn(
                col,
                F.when(F.col(col).isNull() | (F.trim(F.col(col)) == ""), "unknown")
                 .otherwise(F.col(col))
            )

        df = df.withColumn("model_lower", F.lower(F.trim(F.col("model"))))

        inferred_manufacturer = F.lit(None).cast("string")
        for model_keyword, mfr in MANUFACTURER_LOOKUP.items():
            inferred_manufacturer = F.when(
                F.col("model_lower").contains(model_keyword),
                F.lit(mfr)
            ).otherwise(inferred_manufacturer)

        df = df.withColumn(
            "manufacturer",
            F.when(
                F.col("manufacturer").isNull() | (F.col("manufacturer") == ""),
                inferred_manufacturer
            ).otherwise(F.col("manufacturer"))
        ).withColumn(
            "manufacturer",
            F.when(F.col("manufacturer").isNull(), "unknown").otherwise(F.col("manufacturer"))
        ).drop("model_lower")

        # --- 8b. Model ---
        df = df.withColumn(
            "model",
            F.when(F.col("model").isNull() | (F.trim(F.col("model")) == ""), "unknown model")
             .otherwise(F.lower(F.trim(F.col("model"))))
        )

        # --- 8c. Year — Drop null records ---
        df = df.filter(F.col("year").isNotNull() & (F.trim(F.col("year")) != ""))

        # --- 8d. Fuel, Title Status, State → "unknown" ---
        for col in ["fuel", "title_status", "state"]:   # ← remove "transmission" from here
            df = df.withColumn(
                col,
                F.when(F.col(col).isNull() | (F.trim(F.col(col)) == ""), "unknown")
                 .otherwise(F.col(col))
            )

        state_lookup_df = spark.createDataFrame(
                        state_lookup,
                        ["state_code", "state_name"]
                    )


        df = df.withColumn("state", F.upper(F.col("state")))
        df = df.join(
            F.broadcast(state_lookup_df),
            F.col("state") == F.col("state_code"),
            "left"
        )

        df = df.drop("state_code")

        df.select(F.col('state_name')).distinct().show(51) 
        
        # 1. Define two window specifications
        # Window A: Specific grouping (Manufacturer + Model)
        window_model = Window.partitionBy("manufacturer", "model")

        # Window B: Broad fallback grouping (Manufacturer only)
        window_manuf = Window.partitionBy("manufacturer")

        # 2. Calculate the two layers of medians
        median_by_model = F.percentile_approx("odometer", 0.5).over(window_model)
        median_by_manuf = F.percentile_approx("odometer", 0.5).over(window_manuf)

        # 3. Calculate a global median just in case both manufacturer & model are null/unknown
        # (Using a small sample or limit keeps this fast if the dataset is massive)
        global_median_val = df.select(F.percentile_approx("odometer", 0.5)).first()[0]

        # 4. Fill values using a cascading coalesce
        dfd = df.withColumn(
            "odometer",
            F.coalesce(
                F.col("odometer"),        # 1st choice: Original value
                median_by_model,          # 2nd choice: Median of that specific model
                median_by_manuf,          # 3rd choice: Median of the overall manufacturer
                F.lit(global_median_val)  # 4th choice: Global dataset median
            )
        )

        # 1. Standardize "unknown" or empty strings to actual Null values so we can replace them later
        df_clean = df.withColumn(
            "transmission_clean", 
            F.when((F.col("transmission").isNull()) | (F.col("transmission") == "unknown"), None)
             .otherwise(F.col("transmission"))
        )

        # 2. Define a Window to count how often each transmission occurs per manufacturer & model
        window_counts = Window.partitionBy("manufacturer", "model", "transmission_clean")

        # 3. Add a temporary column with the frequency of each valid transmission type
        df_with_counts = df_clean.withColumn("trans_count", F.count("transmission_clean").over(window_counts))

        # 4. Define a Window to rank the frequencies per manufacturer & model
        # We order by trans_count descending. We also order by transmission_clean as a tie-breaker.
        window_rank = Window.partitionBy("manufacturer", "model").orderBy(F.col("trans_count").desc(), F.col("transmission_clean"))

        # 5. Extract the Mode (the value where dense_rank would be 1) 
        # and fallback to a global mode or "automatic" if a manufacturer/model combo has no transmission data at all.
        df_with_mode = df_with_counts.withColumn(
            "mode_transmission", 
            F.first("transmission_clean", ignorenulls=True).over(window_rank)
        ).withColumn(
            "mode_transmission", 
            F.coalesce(F.col("mode_transmission"), F.lit("automatic")) # Fallback if whole group is null
        )

        # 6. Fill the final column: keep existing valid values, otherwise use the calculated mode
        df_final = df_with_mode.withColumn(
            "transmission",
            F.coalesce(F.col("transmission_clean"), F.col("mode_transmission"))
        ).drop("transmission_clean", "trans_count", "mode_transmission") # Clean up temporary columns

        fuzzy_dupes = df.groupBy("manufacturer", "model", "year", "price", "odometer") \
                    .count() \
                    .filter(F.col("count") > 1) \
                    .orderBy(F.desc("count"))

        fuzzy_dupe_groups = fuzzy_dupes.count()
        print(f"  Fuzzy duplicate groups detected (same mfr+model+year+price+odo): {fuzzy_dupe_groups}")
        print("  Note: Fuzzy duplicates reported only — not dropped (legitimate same-spec listings possible).")

        # Price must exist and be positive
        df = df.filter(F.col("price").isNotNull() & (F.col("price") > 0))

        # Year range — dynamic upper bound
        df = df.filter(
            F.col("year").isNotNull() &
            (F.col("year") >= 1980) &
            (F.col("year") <= CURRENT_YEAR)
        )

        df_final = df.filter(F.col("year").isNotNull())
        df = df_final.fillna({
            "VIN": "Not Available", 
            "image_url": "No Image", 
            "description": "No Description"
        })

        silver_df = (df_final
            .withColumn("price", F.col("price").cast("bigint"))
            .withColumn("year", F.col("year").cast("int"))
            .withColumn("odometer", F.col("odometer").cast("bigint"))
        )

        # silver_df = (
        #     df
        #     .withColumn("year_posted", year("posting_date"))
        #     .withColumn("month_posted", month("posting_date"))
        # )

        # silver_df.write\
        #     .mode("overwrite")\
        #     .partitionBy("state_name")\
        #     .parquet(SILVER_PATH)

        silver_df.write \
            .mode("overwrite") \
            .format("parquet") \
            .partitionBy("state_name") \
            .save(SILVER_PATH)
        
        print(silver_df.printSchema())
        print("New Row Count after transformations:", df.count())
        print("Data written to HDFS in Parquet format, partitioned by state.")


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
