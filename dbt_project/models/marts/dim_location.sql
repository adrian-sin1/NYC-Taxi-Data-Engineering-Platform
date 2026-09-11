select
    cast(LocationID as int) as location_id,
    Borough as borough,
    Zone as zone,
    service_zone,
    -- Static display order for the dashboard's "trips by borough" chart --
    -- real boroughs ranked by current trip volume, Unknown/N/A pinned last
    -- regardless of their count. Plain source column (not a Power BI DAX
    -- calculated column) because DirectQuery calculated columns can't use
    -- aggregation functions like COUNTROWS/CALCULATE.
    case Borough
        when 'Manhattan' then 1
        when 'Queens' then 2
        when 'Brooklyn' then 3
        when 'Bronx' then 4
        when 'Staten Island' then 5
        when 'EWR' then 6
        when 'Unknown' then 7
        when 'N/A' then 8
        else 9
    end as borough_sort_order
from read_files(
    's3://nyc-transportation-adrian/reference/taxi_zone_lookup.csv',
    format => 'csv',
    header => true
)
