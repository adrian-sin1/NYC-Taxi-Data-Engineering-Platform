"""Bronze ingestion: read one or more months of raw TLC taxi parquet from
S3, stamp each with ingestion metadata, and write it into its own partition
of the Bronze Delta table via a dynamic partition overwrite (replaceWhere) —
so reprocessing a month is idempotent and other months are untouched.

Run this as a notebook/job on Databricks serverless compute. Edit MONTHS
below for the (year, month) pairs you want to (re)ingest.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

S3_BUCKET = "nyc-transportation-adrian"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/taxi_trips/"

MONTHS = [(2026, 2), (2026, 3), (2026, 4), (2026, 5)]

spark = SparkSession.builder.getOrCreate()


def ingest_month(year: int, month: int) -> None:
    raw_path = f"s3://{S3_BUCKET}/raw/taxi/year={year:04d}/month={month:02d}/"

    raw_df = (
        spark.read.option("pathGlobFilter", "*.parquet")
        .parquet(raw_path)
        .withColumn("ingestion_timestamp", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("year", lit(year))
        .withColumn("month", lit(month))
    )

    (
        raw_df.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"year = {year} AND month = {month}")
        .partitionBy("year", "month")
        .save(BRONZE_PATH)
    )

    print(f"Wrote {raw_df.count()} rows to {BRONZE_PATH} (year={year}, month={month})")


for year, month in MONTHS:
    ingest_month(year, month)
