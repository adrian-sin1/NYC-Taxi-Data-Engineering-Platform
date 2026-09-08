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
    where cast(pickup_datetime as date)
        >= (select coalesce(max(pickup_date), '1900-01-01') from {{ this }})
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
