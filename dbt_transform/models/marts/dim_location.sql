SELECT DISTINCT
    location_id,
    location_name
FROM {{ ref('int_location_clean') }}
WHERE location_id IS NOT NULL