"""Bronze ingestion: read one month of raw TLC taxi parquet from S3, stamp
it with ingestion metadata, and write it into its own partition of the
Bronze Delta table via a dynamic partition overwrite (replaceWhere) — so
reprocessing a month is idempotent and other months are untouched.

Run this as a notebook/job on Databricks serverless compute. Reads
YEAR/MONTH from notebook widgets (set by Airflow when run as a job; default
here to January 2026 so it still works if you just run the cell by hand).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, lit

S3_BUCKET = "nyc-transportation-adrian"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/taxi_trips/"

dbutils.widgets.text("year", "2026")
dbutils.widgets.text("month", "1")
YEAR = int(dbutils.widgets.get("year"))
MONTH = int(dbutils.widgets.get("month"))

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


ingest_month(YEAR, MONTH)
