-- Xu hướng Tuyển dụng và Biến động Lương (30 ngày gần nhất)
SELECT 
    d.date_day AS snapshot_date,
    COUNT(DISTINCT f.job_sk) AS total_active_jobs,
    ROUND(AVG((f.salary_min + f.salary_max) / 2.0) / 1000000, 2) AS avg_market_salary_mil
FROM raw_mart.fct_job_snapshot f
INNER JOIN raw_mart.dim_date d ON f.date_id = d.date_id
WHERE d.date_day >= current_date - INTERVAL '30 days'
  AND f.salary_min > 0 
GROUP BY d.date_day
ORDER BY d.date_day ASC; 