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

WITH mart_sales_analysis__sales AS (
    SELECT 
        sales_order_key
        ,date_key
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
        ,sales_price
        ,sales_amount
        ,currency_code 
    FROM {{ ref('fact_sales_order') }}
    {% if is_incremental() %}
        WHERE order_date >= DATE_SUB((SELECT MAX(order_date) FROM {{ this }}), INTERVAL 1 DAY)
    {% endif %}
),

mart_sales_analysis__product AS (
    SELECT 
        product_key
        ,product_id
        ,product_name
    FROM {{ ref('dim_product') }}
),

mart_sales_analysis__location AS (
    SELECT 
        location_key
        ,country_code
        ,country_name
        ,region_name
        ,city_name
    FROM {{ ref('dim_location') }}
),

mart_sales_analysis__exchange_rate AS (
    SELECT 
        exchange_rate_key
        ,currency_code
        ,exchange_rate
        ,valid_from 
        ,valid_to
        ,is_current
    FROM {{ ref('dim_exchange_rate') }}
),

mart_sales_analysis__date AS (
    SELECT 
        date_key
        ,year
        ,quarter
        ,month
        ,month_name
        ,day
        ,day_of_week
        ,day_name
        ,week_of_year
        ,year_month
        ,is_weekend
    FROM {{ ref('dim_date') }}
)

SELECT 
    -- ==========================================
    -- 1. KEYS
    -- ==========================================
    f.sales_order_key
    ,f.order_id
    ,f.product_key
    -- ==========================================
    -- 2. DATES & TIMES (Phục vụ Time-based trends)
    -- ==========================================
    ,f.order_date
    ,f.order_timestamp
    ,dt.year AS order_year
    ,dt.quarter AS order_quarter
    ,dt.month AS order_month
    ,dt.month_name AS order_month_name
    ,dt.week_of_year AS order_week_of_year
    ,dt.day AS order_day_of_month
    ,dt.day_of_week AS order_day_of_week
    ,dt.day_name AS order_day_name
    ,dt.year_month AS order_year_month
    ,dt.is_weekend
    -- ==========================================
    -- 3. GEOGRAPHIC ATTRIBUTES (Phục vụ Geographic distribution)
    -- ==========================================
    ,COALESCE(l.country_name, 'Unknown') AS country_name
    ,COALESCE(l.city_name, 'Unknown') AS city_name
    -- ==========================================
    -- 4. PRODUCT ATTRIBUTES (Phục vụ Product performance)
    -- ==========================================
    ,COALESCE(p.product_id, 'Unknown') AS product_id
    ,COALESCE(p.product_name, 'Unknown') AS product_name
    ,f.cat_id AS category_id
    -- ==========================================
    -- 5. MEASURES (Phục vụ Revenue analysis)
    -- ==========================================
    ,f.order_qty
    ,f.currency_code
    ,e.is_current
    ,CAST(f.sales_price AS NUMERIC)AS sales_price_original
    ,CAST(f.sales_amount AS NUMERIC) AS sales_amount_original
    -- Quy ước: USD là đơn vị tiền tệ chung. Lấy sales_amount gốc * với tỷ giá để ra USD
    ,CAST(f.sales_price * COALESCE(e.exchange_rate, 1.0) AS NUMERIC) AS sales_price_usd
    ,CAST(f.sales_amount * COALESCE(e.exchange_rate, 1.0) AS NUMERIC) AS sales_amount_usd
FROM mart_sales_analysis__sales AS f
    LEFT JOIN mart_sales_analysis__product AS p ON f.product_key = p.product_key
    LEFT JOIN mart_sales_analysis__location AS l ON f.location_key = l.location_key
    -- BỔ SUNG JOIN VỚI BẢNG dim_date
    LEFT JOIN mart_sales_analysis__date AS dt ON f.date_key = dt.date_key
    LEFT JOIN mart_sales_analysis__exchange_rate AS e 
        ON f.currency_code = e.currency_code
        -- order_date của giao dịch phải nằm trong khoảng hiệu lực của tỷ giá
        AND f.order_date >= e.valid_from 
        AND f.order_date < e.valid_to