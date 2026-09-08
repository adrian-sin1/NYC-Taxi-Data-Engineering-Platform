"""Silver cleaning: process one month of Bronze taxi trips — dedupe, split
invalid rows into quarantine, compute derived columns, join to the taxi zone
lookup — then MERGE the valid rows into the Silver Delta table keyed on a
stable trip_key, so re-running the same month updates rows in place instead
of duplicating them. The quarantine table uses a dynamic partition overwrite
(replaceWhere) for the same idempotency guarantee.

Validation rules: trip_distance > 0, fare_amount >= 0, pickup < dropoff,
PULocationID/DOLocationID within the valid taxi zone range (1-265). Rows
failing any rule go to the quarantine table, not silently dropped.

Run this as a notebook/job on Databricks serverless compute. Edit YEAR/MONTH
below for the month you want to (re)process.
"""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

S3_BUCKET = "nyc-transportation-adrian"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/taxi_trips/"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/taxi_trips/"
QUARANTINE_PATH = f"s3://{S3_BUCKET}/quarantine/taxi_trips/"
ZONE_LOOKUP_PATH = f"s3://{S3_BUCKET}/reference/taxi_zone_lookup.csv"

YEAR = 2026
MONTH = 1

spark = SparkSession.builder.getOrCreate()

bronze_df = spark.read.format("delta").load(BRONZE_PATH).where(
    (F.col("year") == YEAR) & (F.col("month") == MONTH)
)

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
    .withColumn(
        "trip_key",
        F.sha2(
            F.concat_ws(
                "|",
                F.col("VendorID").cast("string"),
                F.col("tpep_pickup_datetime").cast("string"),
                F.col("tpep_dropoff_datetime").cast("string"),
                F.col("PULocationID").cast("string"),
                F.col("DOLocationID").cast("string"),
                F.col("fare_amount").cast("string"),
                F.col("passenger_count").cast("string"),
                F.col("trip_distance").cast("string"),
                F.col("total_amount").cast("string"),
                F.col("tip_amount").cast("string"),
            ),
            256,
        ),
    )
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


def _silver_table_has_trip_key() -> bool:
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        return False
    return "trip_key" in spark.read.format("delta").load(SILVER_PATH).columns


if _silver_table_has_trip_key():
    silver_table = DeltaTable.forPath(spark, SILVER_PATH)
    (
        silver_table.alias("target")
        .merge(silver_df.alias("source"), "target.trip_key = source.trip_key")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    # First-ever write, or migrating an older Silver table that predates
    # trip_key: full overwrite to (re)establish the table with the current
    # schema. Every subsequent run for a different or repeated month uses
    # the MERGE path above.
    silver_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).partitionBy("year", "month").save(SILVER_PATH)

def _quarantine_table_has_failure_reasons() -> bool:
    if not DeltaTable.isDeltaTable(spark, QUARANTINE_PATH):
        return False
    return "failure_reasons" in spark.read.format("delta").load(QUARANTINE_PATH).columns


if _quarantine_table_has_failure_reasons():
    (
        quarantine_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"year = {YEAR} AND month = {MONTH}")
        .partitionBy("year", "month")
        .save(QUARANTINE_PATH)
    )
else:
    # Same migration case as Silver above: the existing quarantine table
    # predates failure_reasons, so replaceWhere's stricter schema check
    # would reject it. Do one full-schema overwrite now; every later run
    # uses replaceWhere.
    quarantine_df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).partitionBy("year", "month").save(QUARANTINE_PATH)

print(f"Merged {silver_df.count()} valid rows into {SILVER_PATH} (year={YEAR}, month={MONTH})")
print(
    f"Wrote {quarantine_df.count()} quarantined rows to {QUARANTINE_PATH} "
    f"(year={YEAR}, month={MONTH})"
)
