WITH dates AS (
    SELECT
        GENERATE_SERIES(
            DATE '2020-01-01',
            DATE '2030-12-31',
            INTERVAL '1 day'
        )::DATE AS date_day
)

SELECT
    TO_CHAR(date_day, 'YYYYMMDD')::INT AS date_id,
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DOW FROM date_day) AS day_of_week,
    TO_CHAR(date_day, 'Month') AS month_name,
    TO_CHAR(date_day, 'Day') AS day_name
FROM dates