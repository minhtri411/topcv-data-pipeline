WITH staging AS (
    SELECT
        job_id,
        LOWER(TRIM(company_url)) AS company_url,
        LOWER(TRIM(company_name_full)) AS company_name,
        company_size,
        company_industry,
        company_website,
        company_address,
        crawled_at
    FROM {{ ref('stg_topcv_jobs') }}
),

with_key AS (
    SELECT
        *,
        {{ dbt_utils.generate_surrogate_key([
            'company_url',
            'company_name'
        ]) }} AS company_id
    FROM staging
),

deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY company_id
            ORDER BY crawled_at DESC NULLS LAST
        ) AS rn
    FROM with_key
)

SELECT
    company_id,
    company_name,
    company_url,
    company_size,
    company_industry,
    company_website,
    company_address,
    crawled_at
FROM deduped
WHERE rn = 1