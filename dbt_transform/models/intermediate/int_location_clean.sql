WITH staging AS (
    SELECT
        LOWER(TRIM(raw_location)) AS location_name
    FROM {{ ref('stg_topcv_jobs') }}
)

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['location_name']) }} AS location_id,
    location_name
FROM staging