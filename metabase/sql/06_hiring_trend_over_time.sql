-- Xu hướng Tuyển dụng và Biến động Lương (30 ngày gần nhất)
WITH latest_date AS (
    SELECT MAX(date_day) AS max_date_day
    FROM raw_mart.dim_date d
    INNER JOIN raw_mart.fct_job_snapshot f ON f.date_id = d.date_id
)
SELECT 
    d.date_day AS "Ngày",
    COUNT(DISTINCT f.job_bk) AS "Số tin đang tuyển",
    ROUND(AVG((f.salary_min + f.salary_max) / 2.0) FILTER (WHERE f.salary_min > 0) / 1000000, 2) AS "Lương trung bình (triệu VNĐ)"
FROM raw_mart.fct_job_snapshot f
INNER JOIN raw_mart.dim_date d ON f.date_id = d.date_id
CROSS JOIN latest_date
WHERE d.date_day >= latest_date.max_date_day - INTERVAL '30 days'
GROUP BY d.date_day
ORDER BY d.date_day ASC;