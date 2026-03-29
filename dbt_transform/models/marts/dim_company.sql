SELECT
    company_id,
    company_name,
    company_url,
    company_size,
    company_industry,
    company_website,
    company_address,
    crawled_at
FROM {{ ref('int_company_clean') }}