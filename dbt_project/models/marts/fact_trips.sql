{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

with trips as (
    select * from {{ ref('stg_taxi_trips') }}

    {% if is_incremental() %}
        {% if var("year", none) and var("month", none) %}
    -- The DAG passes the exact month it's processing, so always
    -- reprocess that one month fully -- correct whether it's new,
    -- already loaded (a refresh after a Silver fix), or older than
    -- what's currently in the table (a backfill). No assumption that
    -- months arrive in chronological order.
    where year = {{ var("year") }} and month = {{ var("month") }}
        {% else %}
    -- Manual `dbt run` with no --vars (ad hoc testing): pick up any
    -- year/month not yet reflected in the table at all, regardless of
    -- where it falls chronologically.
    where (year, month) not in (select distinct year, month from {{ this }})
        {% endif %}
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key([
        'vendor_id', 'pickup_datetime', 'dropoff_datetime',
        'pickup_location_id', 'dropoff_location_id', 'fare_amount',
        'passenger_count', 'trip_distance', 'total_amount', 'tip_amount'
    ]) }} as trip_id,
    vendor_id,
    pickup_datetime,
    dropoff_datetime,
    cast(pickup_datetime as date) as pickup_date,
    pickup_hour,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    rate_code_id,
    store_and_fwd_flag,
    pickup_location_id,
    dropoff_location_id,
    payment_type as payment_type_id,
    fare_amount,
    fare_per_mile,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee
from trips
