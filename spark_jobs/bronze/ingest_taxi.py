"""Bronze ingestion: read one month of raw TLC taxi parquet from S3, stamp it
with ingestion metadata, and write it into that month's partition of the
Bronze Delta table. Uses a dynamic partition overwrite (replaceWhere) so
other months are untouched and re-running the same month is idempotent
rather than duplicating rows.

Run this as a notebook/job on Databricks serverless compute. Edit YEAR/MONTH
below for the month you want to (re)ingest.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

S3_BUCKET = "nyc-transportation-adrian"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/taxi_trips/"

YEAR = 2026
MONTH = 1

spark = SparkSession.builder.getOrCreate()

raw_path = f"s3://{S3_BUCKET}/raw/taxi/year={YEAR:04d}/month={MONTH:02d}/"

raw_df = (
    spark.read.option("pathGlobFilter", "*.parquet")
    .parquet(raw_path)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("year", lit(YEAR))
    .withColumn("month", lit(MONTH))
)

(
    raw_df.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"year = {YEAR} AND month = {MONTH}")
    .partitionBy("year", "month")
    .save(BRONZE_PATH)
)

print(f"Wrote {raw_df.count()} rows to {BRONZE_PATH} (year={YEAR}, month={MONTH})")
