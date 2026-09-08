select
    pickup_date,
    count(*) as trip_count,
    sum(total_amount) as total_revenue,
    avg(fare_amount) as avg_fare,
    avg(trip_distance) as avg_trip_distance,
    avg(trip_duration_minutes) as avg_trip_duration_minutes
from {{ ref('fact_trips') }}
group by pickup_date
order by pickup_date
