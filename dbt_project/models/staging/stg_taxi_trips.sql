with source as (
    select * from {{ source('lakehouse', 'silver_taxi_trips') }}
)

select
    VendorID as vendor_id,
    tpep_pickup_datetime as pickup_datetime,
    tpep_dropoff_datetime as dropoff_datetime,
    passenger_count,
    trip_distance,
    RatecodeID as rate_code_id,
    store_and_fwd_flag,
    PULocationID as pickup_location_id,
    DOLocationID as dropoff_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    trip_duration_minutes,
    pickup_hour,
    fare_per_mile,
    pickup_borough,
    pickup_zone,
    dropoff_borough,
    dropoff_zone,
    year,
    month
from source
