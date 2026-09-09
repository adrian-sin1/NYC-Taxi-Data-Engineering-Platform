{% macro quality_check_month(year, month) %}
  {% set query %}
    select
      (select count(*) from bronze_taxi_trips where year = {{ year }} and month = {{ month }}) as bronze_n,
      (select count(*) from silver_taxi_trips where year = {{ year }} and month = {{ month }}) as silver_n,
      (select count(*) from quarantine_taxi_trips where year = {{ year }} and month = {{ month }}) as quarantine_n
  {% endset %}

  {% set results = run_query(query) %}
  {% if execute %}
    {% set row = results.rows[0] %}
    {% set bronze_n = row['bronze_n'] %}
    {% set silver_n = row['silver_n'] %}
    {% set quarantine_n = row['quarantine_n'] %}
    {% set combined_n = silver_n + quarantine_n %}

    {{ log("Bronze: " ~ bronze_n ~ ", Silver valid: " ~ silver_n ~ ", quarantine: " ~ quarantine_n ~ " (combined: " ~ combined_n ~ ")", info=true) }}

    {% if bronze_n != combined_n %}
      {{ exceptions.raise_compiler_error(
          "Quality check failed for " ~ year ~ "-" ~ month ~ ": Bronze has "
          ~ bronze_n ~ " rows but Silver valid + quarantine only sum to "
          ~ combined_n ~ ". Rows are being lost or duplicated somewhere in Silver."
      ) }}
    {% endif %}
  {% endif %}
{% endmacro %}
