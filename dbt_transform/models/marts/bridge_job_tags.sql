with tag_map as (
    select
        t.job_bk,
        t.snapshot_ts,
        tg.tag_id
    from {{ ref('int_job_tags') }} t
    inner join {{ ref('dim_tag') }} tg
        on t.tag_name_norm = tg.tag_name
),

job_scd as (
    select
        j.job_sk,
        j.external_job_id
    from {{ ref('dim_job') }} j
    where j.dbt_valid_to is null
)

select distinct
    j.job_sk,
    m.tag_id
from tag_map m
inner join job_scd j
    on m.job_bk = j.external_job_id
