-- Bảng xếp hạng Top 15 Doanh nghiệp tuyển dụng nhiều nhất
 WITH latest_date AS (
    SELECT MAX(date_id) AS max_date_id 
    FROM raw_mart.fct_job_snapshot
),
active_jobs AS (
    SELECT 
        c.company_name_full,
        COALESCE(c.company_size, 'Unknown') AS company_size,
        f.job_sk
    FROM raw_mart.fct_job_snapshot f
    INNER JOIN raw_mart.dim_company c ON f.company_sk = c.company_sk
    CROSS JOIN latest_date
    WHERE f.date_id = latest_date.max_date_id
)
SELECT 
    INITCAP(company_name_full) AS company_name,
    company_size,
    COUNT(DISTINCT job_sk) AS total_active_jobs
FROM active_jobs
GROUP BY company_name_full, company_size
ORDER BY total_active_jobs DESC
LIMIT 15;