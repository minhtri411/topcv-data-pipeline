with ranked as (
    select
        job_bk,
        source_id,
        title,
        job_url,
        experience_raw,
        desc_mota,
        desc_yeucau,
        desc_quyenloi,
        working_addresses_raw,
        working_times,
        snapshot_ts,
        row_number() over (
            partition by job_bk
            order by snapshot_ts desc
        ) as rn
    from {{ ref('stg_topcv_jobs') }}
    where job_bk is not null
)

select
    job_bk,
    source_id,
    title,
    job_url,
    experience_raw,
    desc_mota,
    desc_yeucau,
    desc_quyenloi,
    working_addresses_raw,
    working_times,
    snapshot_ts as updated_at
from ranked
where rn = 1
