-- Top 20 Từ khóa/Kỹ năng được săn đón nhất
WITH latest_date AS (
    -- Lấy ngày gần nhất có dữ liệu để chống lỗi "trắng biểu đồ"
    SELECT MAX(date_id) AS max_date_id 
    FROM raw_mart.fct_job_snapshot
),
keyword_data AS (
    SELECT 
        t.tag_name,
        f.job_sk
    FROM raw_mart.fct_job_snapshot f
    INNER JOIN raw_mart.bridge_job_tags b ON f.job_sk = b.job_sk
    INNER JOIN raw_mart.dim_tag t ON b.tag_id = t.tag_id
    CROSS JOIN latest_date
    WHERE f.date_id = latest_date.max_date_id
)
SELECT 
    tag_name AS keyword_name,
    COUNT(DISTINCT job_sk) AS job_count,
    ROUND(100.0 * COUNT(DISTINCT job_sk) / SUM(COUNT(DISTINCT job_sk)) OVER (), 2) AS appearance_percentage
FROM keyword_data
GROUP BY tag_name
ORDER BY job_count DESC
LIMIT 20;
