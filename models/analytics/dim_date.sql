{{ config(materialized='table', schema='analytics') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        start_date="to_date('2016-01-01')",
        end_date="to_date('2019-01-01')",
        datepart="day"
    ) }}
)

SELECT
    date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week,
    EXTRACT(QUARTER FROM date_day) AS quarter
FROM date_spine