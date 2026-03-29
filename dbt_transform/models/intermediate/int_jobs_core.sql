WITH staging AS (
    SELECT
        job_id,
        LOWER(TRIM(job_title)) AS job_title,
        LOWER(TRIM(raw_location)) AS location_name,
        LOWER(TRIM(company_url)) AS company_url,
        LOWER(TRIM(company_name_full)) AS company_name,
        crawled_at
    FROM {{ ref('stg_topcv_jobs') }}
)

SELECT
    job_id,
    job_title,
    location_name,

    -- surrogate keys
    {{ dbt_utils.generate_surrogate_key(['location_name']) }} AS location_id,

    {{ dbt_utils.generate_surrogate_key([
        'company_url',
        'company_name'
    ]) }} AS company_id,

    crawled_at
FROM staging