{% macro register_external_tables() %}
  {% set statements = [
    "CREATE SCHEMA IF NOT EXISTS " ~ target.schema,
    "CREATE TABLE IF NOT EXISTS bronze_taxi_trips USING DELTA LOCATION 's3://nyc-transportation-adrian/bronze/taxi_trips'",
    "CREATE TABLE IF NOT EXISTS silver_taxi_trips USING DELTA LOCATION 's3://nyc-transportation-adrian/silver/taxi_trips'",
    "CREATE TABLE IF NOT EXISTS quarantine_taxi_trips USING DELTA LOCATION 's3://nyc-transportation-adrian/quarantine/taxi_trips'",
    "CREATE TABLE IF NOT EXISTS pipeline_runs (
      run_id STRING,
      run_date TIMESTAMP,
      source_year INT,
      source_month INT,
      records_ingested BIGINT,
      records_processed BIGINT,
      records_quarantined BIGINT,
      records_loaded BIGINT,
      pipeline_status STRING,
      processing_time_seconds DOUBLE
    ) USING DELTA"
  ] %}
  {% for statement in statements %}
    {% do run_query(statement) %}
  {% endfor %}
{% endmacro %}
