{{ config(
    materialized='incremental',
    unique_key='sales_order_key',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    incremental_strategy='insert_overwrite'
) }}

WITH stg_raw AS (
    SELECT 
        order_id
        ,product_id
        ,ip_address
        ,user_id
        ,device_id
        ,cat_id
        ,store_id
        ,order_date
        ,order_timestamp
        ,order_local_datetime
        ,order_qty
        ,sales_price
        ,COALESCE(
            -- Lấy currency đã có của order
            currency
            -- Nếu chưa có: Lấy currency chung của cả order 
            ,MAX(currency) OVER(PARTITION BY order_id)
            -- Nếu cả order không có currency -> đưa về loại 'Unknown'
            ,'Unknown'
        ) AS currency_code
        ,options_list
    FROM {{ ref('stg_raw_data') }}
    -- Lưới lọc Incremental: Cắt đúng phân vùng cần thiết để xử lý
    {% if is_incremental() %}
        WHERE order_date >= DATE_SUB((SELECT MAX(order_date) FROM {{ this }}), INTERVAL 1 DAY)
    {% endif %}
),

ip_mapping AS (
    SELECT 
        ip_address
        ,MAX(country_code) AS country_code
        ,MAX(region_name) AS region_name
        ,MAX(city_name) AS city_name
    FROM {{ ref('stg_ip_locations') }}
    GROUP BY ip_address
),

mapped_raw AS (
    SELECT 
        r.*
        ,COALESCE(m.country_code, 'Unknown') AS loc_country_code
        ,COALESCE(m.region_name, 'Unknown') AS loc_region_name
        ,COALESCE(m.city_name, 'Unknown') AS loc_city_name
    FROM stg_raw AS r
        LEFT JOIN ip_mapping AS m ON r.ip_address = m.ip_address
)

SELECT 
    -- 1. Primary Key 
    {{ dbt_utils.generate_surrogate_key([
        'order_id', 
        'product_id', 
        'CAST(options_list AS STRING)', 
        'CAST(ROW_NUMBER() OVER(PARTITION BY order_id, product_id, CAST(options_list AS STRING) ORDER BY order_timestamp) AS STRING)'
    ]) }} AS sales_order_key
    -- 2. Foreign Keys 
    ,CAST(FORMAT_DATE('%Y%m%d', order_date) AS INT64) AS date_key
    ,{{ dbt_utils.generate_surrogate_key(['product_id']) }} AS product_key
    ,{{ dbt_utils.generate_surrogate_key(['user_id', 'device_id']) }} AS customer_key
    ,{{ dbt_utils.generate_surrogate_key(['loc_country_code', 'loc_region_name', 'loc_city_name']) }} AS location_key   
    -- 3. Natural Keys
    ,order_id  
    ,cat_id
    ,store_id
    -- 4. Dates & Times
    ,order_date 
    ,order_timestamp 
    ,order_local_datetime   
    -- 5. Measures
    ,order_qty 
    ,COALESCE(sales_price, 0) AS sales_price
    ,COALESCE(sales_price, 0) * order_qty AS sales_amount
    -- 6. Descriptive Attributes
    ,currency_code 
    ,options_list AS options_description
FROM mapped_raw
WHERE order_qty > 0