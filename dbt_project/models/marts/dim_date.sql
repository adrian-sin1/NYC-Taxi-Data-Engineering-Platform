with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}
),

holidays as (
    select * from {{ ref('holidays_2026') }}
)

select
    spine.date_day,
    year(spine.date_day) as year,
    month(spine.date_day) as month,
    day(spine.date_day) as day,
    dayofweek(spine.date_day) as day_of_week,
    date_format(spine.date_day, 'EEEE') as day_name,
    date_format(spine.date_day, 'MMMM') as month_name,
    dayofweek(spine.date_day) in (1, 7) as is_weekend,
    holidays.holiday_name is not null as is_holiday,
    holidays.holiday_name
from spine
left join holidays
    on spine.date_day = holidays.holiday_date
