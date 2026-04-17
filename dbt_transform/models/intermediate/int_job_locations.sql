with events as (
    select
        job_bk,
        snapshot_ts,
        location_raw,
        working_addresses_raw
    from {{ ref('int_job_snapshot_events') }}
),

address_items as (
    select
        job_bk,
        snapshot_ts,
        trim(location_item) as location_value
    from events,
    lateral regexp_split_to_table(coalesce(working_addresses_raw, ''), ';') as location_item
),

fallback_location as (
    select
        job_bk,
        snapshot_ts,
        trim(location_raw) as location_value
    from events
    where coalesce(trim(working_addresses_raw), '') = ''
      and coalesce(trim(location_raw), '') <> ''
),

unioned as (
    select * from address_items
    union all
    select * from fallback_location
),

cleaned as (
    select
        job_bk,
        snapshot_ts,
        regexp_replace(
            lower(regexp_replace(location_value, '^[-•\\s]+', '', 'g')),
            '\\s+',
            ' ',
            'g'
        ) as location_name_norm
    from unioned
    where trim(location_value) <> ''
)

select distinct
    job_bk,
    snapshot_ts,
    location_name_norm
from cleaned
where location_name_norm <> ''
