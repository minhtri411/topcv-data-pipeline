-- Phân bố Tin tuyển dụng theo Lĩnh vực hoạt động
WITH latest_date AS (
    SELECT MAX(date_id) as max_date_id FROM raw_mart.fct_job_snapshot
)
SELECT
    COALESCE(NULLIF(TRIM(c.company_industry), ''), 'Không rõ') AS "Lĩnh vực hoạt động",
    COUNT(DISTINCT f.job_sk) AS "Số lượng tin"
FROM raw_mart.fct_job_snapshot f
LEFT JOIN raw_mart.dim_company c ON f.company_sk = c.company_sk
CROSS JOIN latest_date
WHERE f.date_id = latest_date.max_date_id
GROUP BY 1
ORDER BY COUNT(DISTINCT f.job_sk) DESC
LIMIT 15;