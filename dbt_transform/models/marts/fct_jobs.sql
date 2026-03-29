{{ config(
    materialized='incremental',
    unique_key='job_id',
    incremental_strategy='merge',
    on_schema_change='fail',
    merge_update_columns=[
        'job_title',    
        'salary_min',
        'salary_max',
        'currency',
        'avg_salary',
        'crawled_at',
        'date_id',
        'hash_diff'     
    ]
) }}
-- Thêm 'job_title' vào để cho phép update khi HR đổi tên
-- Phải update cả mã hash mới
WITH jobs AS (
    SELECT * FROM {{ ref('int_jobs_core') }}
),

salary AS (
    SELECT * FROM {{ ref('int_salary_parsed') }}
),

final_data AS (
    SELECT
        j.job_id,
        j.company_id,
        j.location_id,
        j.job_title,

        s.salary_min,
        s.salary_max,
        s.currency,

        CASE
            WHEN s.salary_min IS NOT NULL AND s.salary_max IS NOT NULL
                THEN (s.salary_min + s.salary_max) / 2.0 -- Thêm .0 để tránh chia lấy phần nguyên
            ELSE COALESCE(s.salary_min, s.salary_max)
        END AS avg_salary,

        crawled_at,
        TO_CHAR(j.crawled_at, 'YYYYMMDD')::INT AS date_id,

        -- Dùng dbt_utils tự động handle NULL và an toàn hơn CONCAT thuần
        {{ dbt_utils.generate_surrogate_key([
            'j.job_title',
            's.salary_min',
            's.salary_max',
            's.currency'
        ]) }} AS hash_diff

    FROM jobs j
    LEFT JOIN salary s
        ON j.job_id = s.job_id
)

-- Xử lý Incremental bằng LEFT JOIN (Tối ưu hiệu năng cực đại cho Postgres)
SELECT f.* FROM final_data f

{% if is_incremental() %}
    LEFT JOIN {{ this }} t ON f.job_id = t.job_id
    WHERE t.job_id IS NULL            -- Trường hợp 1: Record mới hoàn toàn chưa từng có
       OR f.hash_diff != t.hash_diff  -- Trường hợp 2: Record cũ nhưng có thông tin bị thay đổi
{% endif %}