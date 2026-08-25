WITH latest_date AS (
    SELECT MAX(date_id) AS max_date_id
    FROM raw_mart.fct_job_snapshot
)
SELECT COUNT(DISTINCT f.company_bk) AS total_companies
FROM raw_mart.fct_job_snapshot f
CROSS JOIN latest_date
WHERE f.date_id = latest_date.max_date_id
  AND f.company_sk IS NOT NULL;