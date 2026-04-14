{{
  config(
    materialized='table',
    tags=['static']
  )
}}



WITH date_spine AS (
    SELECT
        DATE_ADD('2000-01-01', INTERVAL n DAY) AS date
    FROM
        UNNEST(GENERATE_ARRAY(0, 10950)) AS n   -- ~30 years
)

SELECT
    date,
    EXTRACT(YEAR FROM date)                     AS year,
    EXTRACT(MONTH FROM date)                    AS month,
    EXTRACT(DAY FROM date)                      AS day,
    FORMAT_DATE('%B', date)                     AS month_name,
    FORMAT_DATE('%b', date)                     AS month_short,
    FORMAT_DATE('%A', date)                     AS day_name,
    FORMAT_DATE('%a', date)                     AS day_short,
    EXTRACT(DAYOFWEEK FROM date)                AS day_of_week,
    EXTRACT(DAYOFYEAR FROM date)                AS day_of_year,
    EXTRACT(QUARTER FROM date)                  AS quarter,
    FORMAT_DATE('Q%Q %Y', date)                 AS quarter_label,
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN TRUE
        ELSE FALSE
    END                                         AS is_weekend,
    CASE
        WHEN EXTRACT(MONTH FROM date) BETWEEN 3 AND 10 THEN TRUE
        ELSE FALSE
    END                                         AS is_baseball_season

FROM date_spine
WHERE date BETWEEN '2000-01-01' AND '2030-12-31'
