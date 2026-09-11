{#
    Permanently deletes one month's data from Bronze, Silver, quarantine,
    and fact_trips. Use this to remove test/backfill months you don't want
    to keep (e.g. a one-off backfill used to validate out-of-order loading).

    The raw source parquet in S3 is untouched, so this is recoverable --
    re-running Bronze/Silver for the same year/month reloads it from S3
    without needing to re-download from TLC.

    Usage:
        dbt run-operation delete_month --args '{"year": 2025, "month": 8}'
#}
{% macro delete_month(year, month) %}
  {% set statements = [
    "DELETE FROM bronze_taxi_trips WHERE year = " ~ year ~ " AND month = " ~ month,
    "DELETE FROM silver_taxi_trips WHERE year = " ~ year ~ " AND month = " ~ month,
    "DELETE FROM quarantine_taxi_trips WHERE year = " ~ year ~ " AND month = " ~ month,
    "DELETE FROM fact_trips WHERE year = " ~ year ~ " AND month = " ~ month
  ] %}
  {% for statement in statements %}
    {{ log("Running: " ~ statement, info=true) }}
    {% do run_query(statement) %}
  {% endfor %}
{% endmacro %}
