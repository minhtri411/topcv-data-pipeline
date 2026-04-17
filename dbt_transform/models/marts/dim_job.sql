with snap as (
    select
        dbt_scd_id,
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
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('snap_dim_job') }}
)

select
    row_number() over (order by job_bk, dbt_valid_from)::bigint as job_sk,
    dbt_scd_id,
    job_bk as external_job_id,
    source_id,
    title,
    job_url,
    experience_raw as experience,
    desc_mota,
    desc_yeucau,
    desc_quyenloi,
    working_addresses_raw as working_addresses,
    working_times,
    dbt_valid_from,
    dbt_valid_to
from snap
