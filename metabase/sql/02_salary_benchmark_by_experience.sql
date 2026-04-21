-- Mặt bằng Lương trung bình theo Cấp bậc (Triệu VNĐ)
WITH latest_date AS (
    SELECT MAX(date_id) as max_date_id FROM raw_mart.fct_job_snapshot
),
clean_data AS (
    SELECT 
        j.experience,
        f.salary_min,
        f.salary_max
    FROM raw_mart.fct_job_snapshot f
    INNER JOIN raw_mart.dim_job j ON f.job_sk = j.job_sk
    CROSS JOIN latest_date
    WHERE f.date_id = latest_date.max_date_id
      AND f.salary_min > 0
      AND j.experience IS NOT NULL
      AND lower(j.experience) NOT IN ('thỏa thuận', 'negotiable', 'không yêu cầu')
)
SELECT 
    experience AS experience_level,
    -- Cột ẩn: Dùng để ép hệ thống xếp đúng thứ tự
    CASE 
        WHEN lower(experience) LIKE '%dưới 1 năm%' THEN 1
        WHEN lower(experience) LIKE '%1 năm%' THEN 2
        WHEN lower(experience) LIKE '%2 năm%' THEN 3
        WHEN lower(experience) LIKE '%3 năm%' THEN 4
        WHEN lower(experience) LIKE '%4 năm%' THEN 5
        WHEN lower(experience) LIKE '%5 năm%' THEN 6
        ELSE 99 
    END AS sort_index,
    COUNT(*) AS job_count,
    ROUND(AVG((salary_min + salary_max) / 2.0) / 1000000, 2) AS avg_salary_million_vnd,
    ROUND(AVG(salary_min) / 1000000, 2) AS min_salary_million_vnd,
    ROUND(AVG(salary_max) / 1000000, 2) AS max_salary_million_vnd
FROM clean_data
GROUP BY experience, sort_index
ORDER BY sort_index ASC; 