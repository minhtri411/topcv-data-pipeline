-- Phân bố Tin tuyển dụng theo Quy mô Doanh nghiệp
WITH latest_date AS (
    SELECT MAX(date_id) as max_date_id FROM raw_mart.fct_job_snapshot
),
clean_data AS (
    SELECT 
        f.job_sk,
        CASE 
            WHEN lower(c.company_size) LIKE '%1-9%' THEN '1-9 nhân viên'
            WHEN lower(c.company_size) LIKE '%10-24%' THEN '10-24 nhân viên'
            WHEN lower(c.company_size) LIKE '%25-99%' THEN '25-99 nhân viên'
            WHEN lower(c.company_size) LIKE '%100-499%' THEN '100-499 nhân viên'
            WHEN lower(c.company_size) LIKE '%500-1000%' THEN '500-1000 nhân viên'
            WHEN lower(c.company_size) LIKE '%1000+%' 
              OR lower(c.company_size) LIKE '%3000+%' 
              OR lower(c.company_size) LIKE '%5000+%' 
              OR lower(c.company_size) LIKE '%10000+%' THEN '1000+ nhân viên'
            ELSE 'Không rõ' 
        END AS clean_company_size,
        CASE 
            WHEN lower(c.company_size) LIKE '%1-9%' THEN 1
            WHEN lower(c.company_size) LIKE '%10-24%' THEN 2
            WHEN lower(c.company_size) LIKE '%25-99%' THEN 3
            WHEN lower(c.company_size) LIKE '%100-499%' THEN 4
            WHEN lower(c.company_size) LIKE '%500-1000%' THEN 5
            WHEN lower(c.company_size) LIKE '%1000+%' 
              OR lower(c.company_size) LIKE '%3000+%' 
              OR lower(c.company_size) LIKE '%5000+%' 
              OR lower(c.company_size) LIKE '%10000+%' THEN 6
            ELSE 99 
        END AS sort_index
    FROM raw_mart.fct_job_snapshot f
    LEFT JOIN raw_mart.dim_company c ON f.company_sk = c.company_sk
    CROSS JOIN latest_date
    WHERE f.date_id = latest_date.max_date_id
)
SELECT 
    clean_company_size AS "Quy mô doanh nghiệp",
    COUNT(DISTINCT job_sk) AS "Số lượng tin"
FROM clean_data
GROUP BY clean_company_size, sort_index
ORDER BY sort_index ASC;