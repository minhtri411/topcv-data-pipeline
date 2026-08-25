WITH latest_date AS (
    SELECT MAX(date_id) AS max_date_id
    FROM raw_mart.fct_job_snapshot
)
SELECT ROUND(AVG((f.salary_min + f.salary_max) / 2.0) / 1000000, 1)::text || ' triệu VNĐ' AS "Lương trung bình"
FROM raw_mart.fct_job_snapshot f
CROSS JOIN latest_date
WHERE f.date_id = latest_date.max_date_id
  AND f.salary_min > 0;