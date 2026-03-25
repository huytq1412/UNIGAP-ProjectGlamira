WITH dim_date__source AS (
    SELECT dt
    FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2030-12-31', INTERVAL 1 DAY)) AS dt
)

SELECT 
    CAST(FORMAT_DATE('%Y%m%d', dt) AS INT64) AS date_key
    ,dt AS full_date
    ,EXTRACT(DAY FROM dt) AS day
    ,EXTRACT(MONTH FROM dt) AS month
    ,EXTRACT(YEAR FROM dt) AS year
    ,EXTRACT(QUARTER FROM dt) AS quarter
    ,EXTRACT(DAYOFWEEK FROM dt) AS day_of_week
    ,EXTRACT(ISOWEEK FROM dt) AS week_of_year
    ,CAST(FORMAT_DATE('%A', dt) AS STRING) AS day_name
    ,CAST(FORMAT_DATE('%B', dt) AS STRING) AS month_name
    ,CAST(FORMAT_DATE('%Y-%m', dt) AS STRING) AS year_month
    ,CASE 
        WHEN EXTRACT(DAYOFWEEK FROM dt) IN (1, 7) THEN TRUE 
        ELSE FALSE 
    END AS is_weekend 
    ,CASE 
        WHEN dt = CURRENT_DATE() THEN TRUE 
        ELSE FALSE 
    END AS is_today
FROM dim_date__source