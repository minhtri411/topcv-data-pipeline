-- Top Khu vực có Nhu cầu Tuyển dụng Data cao nhất
WITH latest_date AS (
    SELECT MAX(date_id) as max_date_id FROM raw_mart.fct_job_snapshot
),
cleaned_locations AS (
    SELECT 
        f.job_sk,
        CASE 
            WHEN lower(l.location_name) LIKE '%hồ chí minh%' OR lower(l.location_name) LIKE '%ho chi minh%' THEN 'TP. Hồ Chí Minh'
            WHEN lower(l.location_name) LIKE '%hà nội%' OR lower(l.location_name) LIKE '%ha noi%' THEN 'Hà Nội'
            WHEN lower(l.location_name) LIKE '%đà nẵng%' OR lower(l.location_name) LIKE '%da nang%' THEN 'Đà Nẵng'
            WHEN lower(l.location_name) LIKE '%cần thơ%' OR lower(l.location_name) LIKE '%can tho%' THEN 'Cần Thơ'
            WHEN lower(l.location_name) LIKE '%bình dương%' OR lower(l.location_name) LIKE '%binh duong%' THEN 'Bình Dương'
            WHEN lower(l.location_name) LIKE '%đồng nai%' OR lower(l.location_name) LIKE '%dong nai%' THEN 'Đồng Nai'
            WHEN lower(l.location_name) LIKE '%hải phòng%' OR lower(l.location_name) LIKE '%hai phong%' THEN 'Hải Phòng'
            WHEN lower(l.location_name) LIKE '%bắc ninh%' OR lower(l.location_name) LIKE '%bac ninh%' THEN 'Bắc Ninh'
            WHEN lower(l.location_name) LIKE '%vũng tàu%' OR lower(l.location_name) LIKE '%vung tau%' THEN 'Bà Rịa - Vũng Tàu'
            WHEN lower(l.location_name) LIKE '%thái nguyên%' OR lower(l.location_name) LIKE '%thai nguyen%' THEN 'Thái Nguyên'
            WHEN lower(l.location_name) LIKE '%huế%' OR lower(l.location_name) LIKE '%hue%' THEN 'Thừa Thiên Huế'
            WHEN lower(l.location_name) LIKE '%remote%' OR lower(l.location_name) LIKE '%từ xa%' THEN 'Remote (Làm việc từ xa)'
            ELSE 'Các tỉnh thành khác' 
        END AS clean_city
    FROM raw_mart.fct_job_snapshot f
    INNER JOIN raw_mart.dim_location l ON f.location_id = l.location_id
    CROSS JOIN latest_date
    WHERE f.date_id = latest_date.max_date_id
)
SELECT 
    clean_city AS city,
    COUNT(DISTINCT job_sk) AS job_count,
    ROUND(100.0 * COUNT(DISTINCT job_sk) / SUM(COUNT(DISTINCT job_sk)) OVER (), 2) AS market_share_percent
FROM cleaned_locations
GROUP BY clean_city
ORDER BY job_count DESC;