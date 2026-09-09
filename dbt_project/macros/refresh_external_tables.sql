{% macro refresh_external_tables() %}
  {% set statements = [
    "REFRESH TABLE bronze_taxi_trips",
    "REFRESH TABLE silver_taxi_trips",
    "REFRESH TABLE quarantine_taxi_trips"
  ] %}
  {% for statement in statements %}
    {% do run_query(statement) %}
  {% endfor %}
{% endmacro %}
