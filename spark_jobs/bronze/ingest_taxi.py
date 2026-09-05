"""Bronze ingestion: read raw TLC taxi parquet from S3, stamp it with
ingestion metadata, and write it to a Bronze Delta table. No cleaning or
validation happens here — that's Silver's job.

Run this as a notebook/job on Databricks serverless compute (it uses the
`nyc-transportation-adrian` Unity Catalog external location for S3 access).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

S3_BUCKET = "nyc-transportation-adrian"
RAW_PATH = f"s3://{S3_BUCKET}/raw/taxi/"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/taxi_trips/"

spark = SparkSession.builder.getOrCreate()

raw_df = (
    spark.read.option("pathGlobFilter", "*.parquet")
    .parquet(RAW_PATH)
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
)

raw_df.write.format("delta").mode("overwrite").partitionBy("year", "month").save(
    BRONZE_PATH
)

print(f"Wrote {raw_df.count()} rows to {BRONZE_PATH}")
