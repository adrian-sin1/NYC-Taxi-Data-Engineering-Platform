with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
),

holidays as (
    select * from {{ ref('holidays_2026') }}
)

select
    cast(spine.date_day as date) as date_day,
    year(spine.date_day) as year,
    month(spine.date_day) as month,
    day(spine.date_day) as day,
    dayofweek(spine.date_day) as day_of_week,
    date_format(spine.date_day, 'EEEE') as day_name,
    date_format(spine.date_day, 'MMMM') as month_name,
    dayofweek(spine.date_day) in (1, 7) as is_weekend,
    holidays.holiday_name is not null as is_holiday,
    holidays.holiday_name,
    -- Sortable numeric key for chronological month ordering across
    -- years (e.g. 202512 for Dec 2025, 202601 for Jan 2026) -- sorting
    -- month_name alone loses year precedence entirely.
    year(spine.date_day) * 100 + month(spine.date_day) as year_month,
    date_format(spine.date_day, 'MMMM yyyy') as year_month_label
from spine
left join holidays
    on spine.date_day = holidays.holiday_date
