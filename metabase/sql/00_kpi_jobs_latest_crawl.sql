WITH latest_date AS (
    SELECT MAX(date_id) AS max_date_id
    FROM raw_mart.fct_job_snapshot
)
SELECT COUNT(DISTINCT f.job_bk) AS jobs_in_latest_crawl
FROM raw_mart.fct_job_snapshot f
CROSS JOIN latest_date
WHERE f.date_id = latest_date.max_date_id;
