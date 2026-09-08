select
    cast(LocationID as int) as location_id,
    Borough as borough,
    Zone as zone,
    service_zone
from read_files(
    's3://nyc-transportation-adrian/reference/taxi_zone_lookup.csv',
    format => 'csv',
    header => true
)
