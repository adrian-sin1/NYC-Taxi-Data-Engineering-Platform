"""One-off Gold checkpoint: aggregate Silver taxi trips into daily metrics.

This is a throwaway proof that the Bronze -> Silver -> Gold chain works end
to end. Phase 2 replaces this with a proper dbt model.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

S3_BUCKET = "nyc-transportation-adrian"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/taxi_trips/"
GOLD_PATH = f"s3://{S3_BUCKET}/gold/daily_trip_metrics/"

spark = SparkSession.builder.getOrCreate()

silver_df = spark.read.format("delta").load(SILVER_PATH)

daily_metrics_df = (
    silver_df.withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
    .groupBy("trip_date")
    .agg(
        F.count("*").alias("trip_count"),
        F.sum("total_amount").alias("total_revenue"),
        F.avg("fare_amount").alias("avg_fare"),
        F.avg("trip_distance").alias("avg_trip_distance"),
        F.avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
    )
    .orderBy("trip_date")
)

daily_metrics_df.write.format("delta").mode("overwrite").save(GOLD_PATH)

daily_metrics_df.show(31, truncate=False)
print(f"Wrote {daily_metrics_df.count()} daily rows to {GOLD_PATH}")
