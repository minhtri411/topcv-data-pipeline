-- Trạng thái xuất hiện của tin
WITH latest_date AS (
    SELECT MAX(date_id) AS max_date_id
    FROM raw_mart.fct_job_snapshot
),
job_last_seen AS (
    SELECT
        job_bk,
        MAX(date_id) AS last_seen_date_id
    FROM raw_mart.fct_job_snapshot
    GROUP BY job_bk
)
SELECT
    CASE
        WHEN last_seen_date_id = (SELECT max_date_id FROM latest_date)
            THEN 'Còn xuất hiện'
        ELSE 'Không còn xuất hiện'
    END AS "Trạng thái (crawl gần nhất)",
    COUNT(*) AS "Số lượng tin"
FROM job_last_seen
GROUP BY 1;