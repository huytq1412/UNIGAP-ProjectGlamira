{% snapshot snap_dim_customer %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_key',
      strategy='timestamp',
      updated_at='last_updated_at'
    )
}}

WITH snap_dim_customer__source AS (
    SELECT 
        user_id
        ,device_id
        ,email_address
        ,order_timestamp
    FROM {{ ref('stg_raw_data') }}
    WHERE user_id IS NOT NULL OR device_id IS NOT NULL
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['user_id', 'device_id']) }} AS customer_key
    ,CAST(user_id AS STRING) AS user_id
    ,CAST(device_id AS STRING) AS device_id
    -- Gom tất cả email thành 1 mảng, sắp xếp theo thời gian mới nhất, và bốc lấy cái đầu tiên
    ,(ARRAY_AGG(CAST(email_address AS STRING) IGNORE NULLS ORDER BY order_timestamp DESC LIMIT 1))[OFFSET(0)] AS email_address
    -- Lấy thời gian giao dịch cuối cùng làm mốc cập nhật cho dbt
    ,MAX(order_timestamp) AS last_updated_at
FROM snap_dim_customer__source
GROUP BY user_id, device_id
-- Xử lý Default record cho các bản ghi không map được
UNION ALL
SELECT 
    {{ dbt_utils.generate_surrogate_key(["'-1'", "'-1'"]) }} AS customer_key
    ,'-1' AS user_id
    ,'-1' AS device_id
    ,'Unknown' AS email_address
    ,CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS last_updated_at

{% endsnapshot %}