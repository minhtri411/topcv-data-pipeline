{{ config(
    materialized='incremental',
    unique_key='job_id'
) }}

SELECT
    job_id,
    desc_mota,
    desc_yeucau,
    desc_quyenloi,
    working_addresses,
    working_times,
    tags,
    crawled_at
FROM {{ ref('stg_topcv_jobs') }}

{% if is_incremental() %}
    WHERE crawled_at >= (
        SELECT COALESCE(MAX(crawled_at), '1900-01-01'::timestamp)
        FROM {{ this }}
    )
{% endif %}