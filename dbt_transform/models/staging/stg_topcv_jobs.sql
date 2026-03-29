WITH source AS(
    SELECT * FROM {{ source('topcv_raw', 'topcv_jobs')}}
    WHERE job_url IS NOT NULL
),

deduped AS (
    SELECT 
        {{ dbt_utils.generate_surrogate_key(['job_url'])}} as job_id,
        title as job_title,
        LOWER(TRIM(job_url)) as job_url,

        company_url,

        salary as raw_salary,
        location as raw_location,
        experience as raw_experience,
        deadline,
        tags,
        desc_mota,
        desc_yeucau,
        desc_quyenloi,

        working_addresses,
        working_times,

        company_name_full,
        company_website,
        company_size,
        company_followers,
        company_industry,
        company_address,
        company_description,
        crawled_at,

        ROW_NUMBER() OVER(
            PARTITION BY LOWER(TRIM(job_url))
            ORDER BY crawled_at DESC NULLS LAST
        ) as rn
    FROM source
)

SELECT * 
FROM deduped
WHERE rn = 1
