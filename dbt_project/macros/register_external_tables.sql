{% macro register_external_tables() %}
  {% set statements = [
    "CREATE SCHEMA IF NOT EXISTS " ~ target.schema,
    "CREATE TABLE IF NOT EXISTS bronze_taxi_trips USING DELTA LOCATION 's3://nyc-transportation-adrian/bronze/taxi_trips'",
    "CREATE TABLE IF NOT EXISTS silver_taxi_trips USING DELTA LOCATION 's3://nyc-transportation-adrian/silver/taxi_trips'",
    "CREATE TABLE IF NOT EXISTS quarantine_taxi_trips USING DELTA LOCATION 's3://nyc-transportation-adrian/quarantine/taxi_trips'"
  ] %}
  {% for statement in statements %}
    {% do run_query(statement) %}
  {% endfor %}
{% endmacro %}
