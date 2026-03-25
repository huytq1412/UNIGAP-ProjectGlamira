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

WITH fact_sales_order__raw AS (
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
    FROM {{ ref('stg_raw_data') }}
    -- Lưới lọc Incremental: Cắt đúng phân vùng cần thiết để xử lý
    {% if is_incremental() %}
        WHERE order_date >= DATE_SUB((SELECT MAX(order_date) FROM {{ this }}), INTERVAL 1 DAY)
    {% endif %}
),

fact_sales_order__ip_mapping AS (
    SELECT 
        ip_address
        ,MAX(location_key) AS location_key
    FROM {{ ref('stg_ip_locations') }}
    GROUP BY ip_address
),

fact_sales_order__mapped AS (
    SELECT 
        r.*
        ,m.location_key
    FROM fact_sales_order__raw AS r
        LEFT JOIN fact_sales_order__ip_mapping AS m ON r.ip_address = m.ip_address
)

SELECT 
    -- 1. Primary Key 
    {{ dbt_utils.generate_surrogate_key([
        'order_id', 
        'COALESCE(CAST(product_id AS STRING), \'-1\')', 
        'CAST(ROW_NUMBER() OVER(PARTITION BY order_id, COALESCE(CAST(product_id AS STRING), \'-1\') ORDER BY order_timestamp) AS STRING)'
    ]) }} AS sales_order_key
    -- 2. Foreign Keys 
    ,CAST(FORMAT_DATE('%Y%m%d', order_date) AS INT64) AS date_key 
    -- XỬ LÝ DEFAULT VALUE: Nếu thiếu product_id, ép về '-1' để JOIN khớp với dòng Default trong dim_product
    ,{{ dbt_utils.generate_surrogate_key(['COALESCE(CAST(product_id AS STRING), \'-1\')']) }} AS product_key
    -- XỬ LÝ DEFAULT VALUE: Nếu thiếu user/device, ép về '-1' để JOIN khớp với snap_dim_Customer
    ,{{ dbt_utils.generate_surrogate_key(['COALESCE(CAST(user_id AS STRING), \'-1\')', 'COALESCE(CAST(device_id AS STRING), \'-1\')']) }} AS customer_key
    -- XỬ LÝ DEFAULT VALUE: Nếu không map được IP, ép về  'Unknown để JOIN khớp với dim_location
    ,COALESCE(location_key, {{ dbt_utils.generate_surrogate_key(["'Unknown'", "'Unknown'", "'Unknown'"]) }}) AS location_key
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
FROM fact_sales_order__mapped
WHERE order_qty > 0