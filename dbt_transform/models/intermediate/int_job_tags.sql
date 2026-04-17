with base as (
    select
        job_bk,
        snapshot_ts,
        tags_raw
    from {{ ref('int_job_snapshot_events') }}
),

expanded as (
    select
        job_bk,
        snapshot_ts,
        trim(tag_item) as tag_name_raw
    from base,
    lateral regexp_split_to_table(coalesce(tags_raw, ''), ';') as tag_item
),

cleaned as (
    select
        job_bk,
        snapshot_ts,
        lower(regexp_replace(tag_name_raw, '\\s+', ' ', 'g')) as tag_name_norm
    from expanded
    where trim(tag_name_raw) <> ''
)

select distinct
    job_bk,
    snapshot_ts,
    tag_name_norm
from cleaned
