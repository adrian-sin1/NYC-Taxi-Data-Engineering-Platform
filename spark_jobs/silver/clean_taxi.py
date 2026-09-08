"""Silver cleaning: dedupe Bronze taxi trips, split out invalid rows into a
quarantine table, compute derived columns, and join to the taxi zone lookup
for borough/zone names.

Validation rules: trip_distance > 0, fare_amount >= 0, pickup < dropoff,
PULocationID/DOLocationID within the valid taxi zone range (1-265). Rows
failing any rule go to the quarantine table, not silently dropped.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

S3_BUCKET = "nyc-transportation-adrian"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/taxi_trips/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/taxi_trips/"
QUARANTINE_PATH = f"s3://{S3_BUCKET}/quarantine/taxi_trips/"
ZONE_LOOKUP_PATH = f"s3://{S3_BUCKET}/reference/taxi_zone_lookup.csv"

spark = SparkSession.builder.getOrCreate()

bronze_df = spark.read.format("delta").load(BRONZE_PATH)

# Dedupe on business columns only, so re-ingesting the same trips (which get
# a fresh ingestion_timestamp/source_file each run) still counts as duplicates.
business_columns = [
    c for c in bronze_df.columns if c not in ("ingestion_timestamp", "source_file")
]
deduped_df = bronze_df.dropDuplicates(business_columns)

typed_df = (
    deduped_df.withColumn("VendorID", F.col("VendorID").cast(IntegerType()))
    .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
    .withColumn("RatecodeID", F.col("RatecodeID").cast(IntegerType()))
    .withColumn("PULocationID", F.col("PULocationID").cast(IntegerType()))
    .withColumn("DOLocationID", F.col("DOLocationID").cast(IntegerType()))
    .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
)

has_positive_distance = F.col("trip_distance") > 0
has_nonnegative_fare = F.col("fare_amount") >= 0
has_valid_time_order = F.col("tpep_pickup_datetime") < F.col("tpep_dropoff_datetime")
has_valid_pickup_location = F.col("PULocationID").between(1, 265)
has_valid_dropoff_location = F.col("DOLocationID").between(1, 265)

is_valid = (
    has_positive_distance
    & has_nonnegative_fare
    & has_valid_time_order
    & has_valid_pickup_location
    & has_valid_dropoff_location
)

failure_reasons = F.array_compact(
    F.array(
        F.when(~has_positive_distance, F.lit("trip_distance_not_positive")),
        F.when(~has_nonnegative_fare, F.lit("fare_amount_negative")),
        F.when(~has_valid_time_order, F.lit("pickup_not_before_dropoff")),
        F.when(~has_valid_pickup_location, F.lit("invalid_pickup_location_id")),
        F.when(~has_valid_dropoff_location, F.lit("invalid_dropoff_location_id")),
    )
)

valid_df = typed_df.filter(is_valid)
quarantine_df = typed_df.filter(~is_valid).withColumn(
    "failure_reasons", failure_reasons
)

zone_lookup = spark.read.option("header", True).csv(ZONE_LOOKUP_PATH)
pickup_zones = zone_lookup.select(
    F.col("LocationID").cast(IntegerType()).alias("PULocationID"),
    F.col("Borough").alias("pickup_borough"),
    F.col("Zone").alias("pickup_zone"),
)
dropoff_zones = zone_lookup.select(
    F.col("LocationID").cast(IntegerType()).alias("DOLocationID"),
    F.col("Borough").alias("dropoff_borough"),
    F.col("Zone").alias("dropoff_zone"),
)

silver_df = (
    valid_df.withColumn(
        "trip_duration_minutes",
        (
            F.unix_timestamp("tpep_dropoff_datetime")
            - F.unix_timestamp("tpep_pickup_datetime")
        )
        / 60,
    )
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("fare_per_mile", F.col("fare_amount") / F.col("trip_distance"))
    .join(pickup_zones, on="PULocationID", how="left")
    .join(dropoff_zones, on="DOLocationID", how="left")
)

silver_df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(
    SILVER_PATH
)
quarantine_df.write.format("delta").mode("overwrite").partitionBy(
    "year", "month"
).save(QUARANTINE_PATH)

print(f"Wrote {silver_df.count()} valid rows to {SILVER_PATH}")
print(f"Wrote {quarantine_df.count()} quarantined rows to {QUARANTINE_PATH}")
