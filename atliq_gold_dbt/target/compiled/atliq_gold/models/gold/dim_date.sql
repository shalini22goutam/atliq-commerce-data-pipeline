

WITH date_spine AS (

    SELECT
        explode(
            sequence(
                to_date('2024-01-01'),
                to_date('2026-12-31'),
                interval 1 day
            )
        ) AS date_day

)

SELECT
    date_day,
    CAST(date_format(date_day, 'yyyyMMdd') AS INT) AS date_key,
    year(date_day) AS year,
    quarter(date_day) AS quarter,
    month(date_day) AS month,
    monthname(date_day) AS month_name,
    weekofyear(date_day) AS week_of_year,
    dayofmonth(date_day) AS day_of_month,
    dayofweek(date_day) AS day_of_week,
    dayname(date_day) AS day_name

FROM date_spine