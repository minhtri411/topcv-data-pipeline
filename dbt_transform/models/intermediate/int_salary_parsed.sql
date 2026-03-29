WITH staging AS (
    SELECT
        job_id,
        raw_salary
    FROM {{ ref('stg_topcv_jobs') }}
),

base AS (
    SELECT
        job_id,
        raw_salary,

        -- detect currency
        CASE
            WHEN raw_salary ILIKE '%usd%' OR raw_salary LIKE '%$%' THEN 'USD'
            WHEN raw_salary ILIKE '%triệu%' 
              OR raw_salary ILIKE '%vnđ%' 
              OR raw_salary ILIKE '%vnd%' THEN 'VND'
            ELSE 'UNKNOWN'
        END AS currency
    FROM staging
),

parsed AS (
    SELECT
        job_id,
        currency,

        -- salary_min
        CASE
            WHEN raw_salary IS NULL THEN NULL
            WHEN raw_salary ILIKE '%thỏa thuận%' THEN NULL

            WHEN raw_salary ILIKE '%-%' THEN 
                NULLIF(REGEXP_REPLACE(
                    SPLIT_PART(raw_salary, '-', 1),
                    '[^0-9\.]', '', 'g'
                ), '')::NUMERIC

            WHEN raw_salary ILIKE '%trên%' THEN 
                NULLIF(REGEXP_REPLACE(raw_salary, '[^0-9\.]', '', 'g'), '')::NUMERIC

            WHEN raw_salary ILIKE '%tới%' THEN 0

            ELSE 
                NULLIF(REGEXP_REPLACE(raw_salary, '[^0-9\.]', '', 'g'), '')::NUMERIC
        END AS salary_min_raw,

        -- salary_max
        CASE
            WHEN raw_salary IS NULL THEN NULL
            WHEN raw_salary ILIKE '%thỏa thuận%' THEN NULL

            WHEN raw_salary ILIKE '%-%' THEN 
                NULLIF(REGEXP_REPLACE(
                    SPLIT_PART(raw_salary, '-', 2),
                    '[^0-9\.]', '', 'g'
                ), '')::NUMERIC

            WHEN raw_salary ILIKE '%tới%' THEN 
                NULLIF(REGEXP_REPLACE(raw_salary, '[^0-9\.]', '', 'g'), '')::NUMERIC

            WHEN raw_salary ILIKE '%trên%' THEN NULL

            ELSE 
                NULLIF(REGEXP_REPLACE(raw_salary, '[^0-9\.]', '', 'g'), '')::NUMERIC
        END AS salary_max_raw
    FROM base
),

normalized AS (
    SELECT
        job_id,
        currency,

        -- normalize to VND
        CASE
            WHEN currency = 'VND' THEN salary_min_raw * 1000000
            WHEN currency = 'USD' THEN salary_min_raw * 26000
            ELSE salary_min_raw
        END AS salary_min,

        CASE
            WHEN currency = 'VND' THEN salary_max_raw * 1000000
            WHEN currency = 'USD' THEN salary_max_raw * 26000
            ELSE salary_max_raw
        END AS salary_max
    FROM parsed
)

SELECT * FROM normalized