with bounds as (
    select
        min(snapshot_ts::date) as min_date,
        max(snapshot_ts::date) as max_date
    from {{ ref('int_job_snapshot_events') }}
),

calendar as (
    select
        generate_series(
            coalesce(min_date, current_date - interval '365 day')::date,
            coalesce(max_date, current_date + interval '365 day')::date,
            interval '1 day'
        )::date as date_day
    from bounds
)

select
    cast(to_char(date_day, 'YYYYMMDD') as integer) as date_id,
    date_day,
    extract(day from date_day)::int as day_of_month,
    extract(month from date_day)::int as month_of_year,
    extract(year from date_day)::int as year_number,
    extract(isodow from date_day)::int as iso_day_of_week,
    case when extract(isodow from date_day) in (6, 7) then true else false end as is_weekend
from calendar
