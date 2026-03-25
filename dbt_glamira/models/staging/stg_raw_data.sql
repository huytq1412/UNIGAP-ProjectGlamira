{{ config(
    materialized='incremental',
    partition_by={
      "field": "order_date",
      "data_type": "date",
      "granularity": "day"
    },
    incremental_strategy='insert_overwrite'
) }}

WITH stg_raw_data__source AS (
      SELECT 
            order_id,
            time_stamp,
            local_time,
            ip,
            user_id_db AS user_id,
            device_id,
            cat_id,
            store_id,
            email_address,
            cart_products
      FROM {{ source('raw_layer_avro', 'raw_data') }}
      WHERE collection = 'checkout_success' AND order_id IS NOT NULL

      -- Chỉ lấy dữ liệu của ngày hôm qua và hôm nay
      {% if is_incremental() %}
        AND DATE(TIMESTAMP_SECONDS(time_stamp)) >= DATE_SUB((SELECT MAX(order_date) FROM {{ this }}), INTERVAL 1 DAY)
      {% endif %}
),
stg_raw_data__unnested AS (
      SELECT 
            order_id
            ,time_stamp
            ,local_time
            ,ip
            ,user_id
            ,device_id
            ,cat_id
            ,store_id
            ,email_address
            ,product.product_id AS product_id
            ,product.amount AS quantity
            ,product.currency AS currency
            ,product.option AS options_list   
            -- Xử lý làm sạch giá tiền bằng 2 nhóm Regex độc lập:
            -- Nhóm 1 (Bên trong): Xóa sạch các ký tự rác (như ', khoảng trắng, -, _)
            -- Nhóm 2 (Bọc ngoài): Đổi các dấu thập phân lạ (như ٫) thành dấu chấm (.)
            ,REGEXP_REPLACE(REGEXP_REPLACE(product.price, r"[' \-_]", ""), r"[٫]", ".") AS clean_price_str
      FROM stg_raw_data__source
      -- Sử dụng LEFT JOIN UNNEST nguyên bản của BigQuery
      LEFT JOIN UNNEST(cart_products) AS product
)

SELECT 
      SPLIT(order_id, '.')[OFFSET(0)] AS order_id
      ,TIMESTAMP_SECONDS(time_stamp) AS order_timestamp
      ,CAST(local_time AS DATETIME) AS order_local_datetime
      ,DATE(TIMESTAMP_SECONDS(time_stamp)) AS order_date
      ,ip AS ip_address
      ,user_id
      ,device_id
      ,cat_id
      ,store_id
      ,email_address
      ,product_id
      ,CAST(quantity AS INT64) AS order_qty
      ,SAFE_CAST(
        CASE
          -- Trường hợp 1: Nếu có BẤT KỲ ký tự nào ngoài số, chấm, phẩy -> Cho bằng NULL lập tức.
          WHEN REGEXP_CONTAINS(clean_price_str, r'[^0-9.,]') THEN NULL        
          -- Trường hợp 2: Dấu phẩy nằm sau dấu chấm, hoặc chỉ có dấu phẩy (VD: 2.365,00 hoặc 257,00)
          WHEN STRPOS(clean_price_str, ',') > STRPOS(clean_price_str, '.') 
            THEN REPLACE(REPLACE(clean_price_str, '.', ''), ',', '.')
          -- Trường hợp 3: Dấu chấm nằm sau dấu phẩy, hoặc chỉ có dấu chấm (VD: 2,365.00 hoặc 322.00)
          ELSE 
            REPLACE(clean_price_str, ',', '')
        END 
        AS NUMERIC) AS sales_price
      ,CASE 
        -- Gom đơn vị USD $ về chung thành $
        WHEN TRIM(currency) = 'USD $' THEN '$' 
        ELSE NULLIF(TRIM(currency), '') 
      END AS currency
      ,options_list
FROM stg_raw_data__unnested