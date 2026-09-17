{#
    Records one row of pipeline observability data per DAG run -- how many
    rows moved through Bronze/Silver/quarantine/fact_trips for the month
    just processed, how long the run took, and whether it succeeded. Called
    as the last task in the Airflow DAG, after dbt_test, so Power BI can
    report on pipeline health over time instead of just the taxi data
    itself.

    Usage:
        dbt run-operation log_pipeline_run --args '{"run_id": "...", "year": 2026, "month": 5, "processing_time_seconds": 842.3, "pipeline_status": "SUCCESS"}'
#}
{% macro log_pipeline_run(run_id, year, month, processing_time_seconds, pipeline_status) %}
  {% set counts_query %}
    select
      (select count(*) from bronze_taxi_trips where year = {{ year }} and month = {{ month }}) as records_ingested,
      (select count(*) from silver_taxi_trips where year = {{ year }} and month = {{ month }}) as records_processed,
      (select count(*) from quarantine_taxi_trips where year = {{ year }} and month = {{ month }}) as records_quarantined,
      (select count(*) from fact_trips where year = {{ year }} and month = {{ month }}) as records_loaded
  {% endset %}

  {% set results = run_query(counts_query) %}
  {% if execute %}
    {% set row = results.rows[0] %}
    {% set insert_statement %}
      insert into pipeline_runs values (
        '{{ run_id }}',
        current_timestamp(),
        {{ year }},
        {{ month }},
        {{ row['records_ingested'] }},
        {{ row['records_processed'] }},
        {{ row['records_quarantined'] }},
        {{ row['records_loaded'] }},
        '{{ pipeline_status }}',
        {{ processing_time_seconds }}
      )
    {% endset %}
    {% do run_query(insert_statement) %}
    {{ log(
        "Logged pipeline run " ~ run_id ~ " for " ~ year ~ "-" ~ month
        ~ ": status=" ~ pipeline_status ~ ", time=" ~ processing_time_seconds ~ "s",
        info=true
    ) }}
  {% endif %}
{% endmacro %}
