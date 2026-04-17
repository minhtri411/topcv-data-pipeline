WITH latest_date AS (
    SELECT MAX(date_id) as max_date_id FROM raw_mart.fct_job_snapshot
),
clean_data AS (
    SELECT 
        f.job_sk,
        CASE 
            WHEN lower(c.company_size) LIKE '%1-9%' THEN '1-9 nhân viên'
            WHEN lower(c.company_size) LIKE '%10-24%' THEN '10-24 nhân viên'
            WHEN lower(c.company_size) LIKE '%25-99%' THEN '25-99 nhân viên'
            WHEN lower(c.company_size) LIKE '%100-499%' THEN '100-499 nhân viên'
            WHEN lower(c.company_size) LIKE '%500-1000%' THEN '500-1000 nhân viên'
            WHEN lower(c.company_size) LIKE '%1000+%' 
              OR lower(c.company_size) LIKE '%3000+%' 
              OR lower(c.company_size) LIKE '%5000+%' 
              OR lower(c.company_size) LIKE '%10000+%' THEN '1000+ nhân viên'
            ELSE 'Unknown' 
        END AS clean_company_size,
        
        CASE 
            WHEN lower(c.company_size) LIKE '%1-9%' THEN 1
            WHEN lower(c.company_size) LIKE '%10-24%' THEN 2
            WHEN lower(c.company_size) LIKE '%25-99%' THEN 3
            WHEN lower(c.company_size) LIKE '%100-499%' THEN 4
            WHEN lower(c.company_size) LIKE '%500-1000%' THEN 5
            WHEN lower(c.company_size) LIKE '%1000+%' 
              OR lower(c.company_size) LIKE '%3000+%' 
              OR lower(c.company_size) LIKE '%5000+%' 
              OR lower(c.company_size) LIKE '%10000+%' THEN 6
            ELSE 99 
        END AS sort_index
        
    FROM raw_mart.fct_job_snapshot f
    INNER JOIN raw_mart.dim_company c ON f.company_sk = c.company_sk
    CROSS JOIN latest_date
    WHERE f.date_id = latest_date.max_date_id
)
SELECT 
    clean_company_size AS company_size,
    COUNT(DISTINCT job_sk) AS job_count,
    ROUND(100.0 * COUNT(DISTINCT job_sk) / SUM(COUNT(DISTINCT job_sk)) OVER (), 2) AS hiring_share_percent,
    sort_index -- Keep this hidden field for deterministic ordering.
FROM clean_data
GROUP BY clean_company_size, sort_index
ORDER BY sort_index ASC;