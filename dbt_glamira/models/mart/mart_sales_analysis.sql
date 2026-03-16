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

WITH fact_sales AS (
    SELECT 
        sales_order_key
        ,time_key
        ,product_key
        ,customer_key
        ,location_key     
        ,order_id  
        ,cat_id
        ,store_id
        ,order_date 
        ,order_timestamp 
        ,order_local_datetime   
        ,order_qty 
        ,sales_amount
        ,currency_code 
        ,options_description
    FROM {{ ref('fact_sales_order') }}
    {% if is_incremental() %}
        WHERE order_date >= DATE_SUB((SELECT MAX(order_date) FROM {{ this }}), INTERVAL 1 DAY)
    {% endif %}
),

dim_product AS (
    SELECT 
        product_key
        ,product_id
        ,product_name
    FROM {{ ref('dim_product') }}
),

dim_location AS (
    SELECT 
        location_key   
        ,ip_address  
        ,country_code
        ,country_name
        ,region_name
        ,city_name
    FROM {{ ref('dim_location') }}
),

dim_exchange_rate AS (
    SELECT 
        exchange_rate_key
        ,currency_code
        ,exchange_rate
        ,valid_from 
        ,valid_to
        ,is_current
    FROM {{ ref('dim_exchange_rate') }}
)

SELECT 
    -- ==========================================
    -- 1. IDENTIFIERS (Keys & Codes)
    -- ==========================================
    f.sales_order_key
    ,f.order_id
    ,f.product_key
    -- ==========================================
    -- 2. DATES & TIMES (Phục vụ Time-based trends)
    -- ==========================================
    ,f.order_date
    ,f.order_timestamp
    -- ==========================================
    -- 3. GEOGRAPHIC ATTRIBUTES (Phục vụ Geographic distribution)
    -- ==========================================
    ,COALESCE(l.country_name, 'Unknown') AS country_name
    ,COALESCE(l.city_name, 'Unknown') AS city_name
    -- ==========================================
    -- 4. PRODUCT ATTRIBUTES (Phục vụ Product performance)
    -- ==========================================
    ,COALESCE(p.product_id, 'Unknown') AS product_code
    ,COALESCE(p.product_name, 'Unknown') AS product_name
    ,f.cat_id AS category_id
    -- ==========================================
    -- 5. MEASURES (Phục vụ Revenue analysis)
    -- ==========================================
    ,f.order_qty
    ,f.currency_code
    ,CAST(f.sales_amount AS NUMERIC) AS sales_amount_original
    -- Quy ước: USD là đơn vị tiền tệ chung. Lấy sales_amount gốc * với tỷ giá để ra USD
    ,CAST(f.sales_amount * COALESCE(e.exchange_rate, 1.0) AS NUMERIC) AS sales_amount_usd
FROM fact_sales AS f
    LEFT JOIN dim_product AS p ON f.product_key = p.product_key
    LEFT JOIN dim_location AS l ON f.location_key = l.location_key
    LEFT JOIN dim_exchange_rate AS e 
        ON f.currency_code = e.currency_code
        -- order_date của giao dịch phải nằm trong khoảng hiệu lực của tỷ giá
        AND f.order_date >= e.valid_from 
        AND f.order_date < COALESCE(e.valid_to, CAST('9999-12-31' AS DATE))