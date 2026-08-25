-- Bảng xếp hạng Top 10 Doanh nghiệp tuyển dụng nhiều nhất
 WITH latest_date AS (
    SELECT MAX(date_id) AS max_date_id 
    FROM raw_mart.fct_job_snapshot
),
active_jobs AS (
    SELECT 
        COALESCE(c.company_name_full, 'Không xác định công ty') AS company_name_full,
        COALESCE(c.company_size, 'Không rõ') AS company_size,
        f.job_sk
    FROM raw_mart.fct_job_snapshot f
    LEFT JOIN raw_mart.dim_company c ON f.company_sk = c.company_sk
    CROSS JOIN latest_date
    WHERE f.date_id = latest_date.max_date_id
)
SELECT 
    INITCAP(company_name_full) AS "Tên doanh nghiệp",
    company_size AS "Quy mô",
    COUNT(DISTINCT job_sk) AS "Số tin đang tuyển"
FROM active_jobs
GROUP BY company_name_full, company_size
ORDER BY COUNT(DISTINCT job_sk) DESC
LIMIT 10;