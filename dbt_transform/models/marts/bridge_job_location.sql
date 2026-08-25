with location_map as (
    select
        l.job_bk,
        l.snapshot_ts,
        dl.location_id
    from {{ ref('int_job_locations') }} l
    inner join {{ ref('dim_location') }} dl
        on l.location_name_norm = dl.location_code
),

job_scd as (
    select
        j.job_sk,
        j.external_job_id,
        j.dbt_valid_from,
        j.dbt_valid_to
    from {{ ref('dim_job') }} j
)

select distinct
    j.job_sk,
    m.location_id
from location_map m
inner join job_scd j
    on m.job_bk = j.external_job_id
   and m.snapshot_ts >= j.dbt_valid_from
   and m.snapshot_ts < coalesce(j.dbt_valid_to, 'infinity'::timestamp)