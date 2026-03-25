WITH stg_exchange_rates__source AS (
    SELECT 
        currency_code
        ,rate_date
        ,exchange_rate
    FROM {{ ref('exchange_rates') }}
)
,stg_exchange_rates__grouped AS (
    -- Dùng GROUP BY để chặn rủi ro file Seed có 2 dòng trùng ngày
    SELECT 
        CAST(TRIM(currency_code) AS STRING) AS currency_code
        ,CAST(rate_date AS DATE) AS rate_date
        ,CAST(MAX(exchange_rate) AS NUMERIC) AS exchange_rate
    FROM stg_exchange_rates__source
    GROUP BY currency_code, rate_date
)
SELECT 
    currency_code
    ,exchange_rate
    ,rate_date AS valid_from
    -- Lấy ngày rate_date của dòng tỷ giá tiếp theo làm ngày hết hạn. Nếu là dòng mới nhất (không có dòng tiếp theo) -> trả về NULL.
    ,LEAD(rate_date) OVER(PARTITION BY currency_code ORDER BY rate_date) AS valid_to
FROM stg_exchange_rates__grouped