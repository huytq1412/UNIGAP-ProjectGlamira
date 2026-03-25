WITH dim_product__event_products AS (
    -- Lấy danh sách product_id từ event thực
    SELECT product_id
    FROM {{ ref('stg_raw_data') }}
    WHERE product_id IS NOT NULL
    GROUP BY product_id
),
dim_product__product_names AS (
    -- Lấy danh sách sản phẩm từ danh mục
    SELECT 
        product_id
        ,product_name
    FROM {{ ref('stg_product_names') }}
    WHERE product_id IS NOT NULL
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['COALESCE(f.product_id, d.product_id)']) }} AS product_key
    ,CAST(COALESCE(f.product_id, d.product_id) AS STRING) AS product_id
    -- Nếu code map được với danh mục thì lấy tên, không map được thì gán nhãn Unknown
    ,CAST(COALESCE(d.product_name, 'Unknown') AS STRING) AS product_name
FROM dim_product__event_products AS f
    FULL OUTER JOIN dim_product__product_names AS d ON f.product_id = d.product_id
-- Xử lý Default record cho các product_id không map được
UNION ALL
SELECT 
    {{ dbt_utils.generate_surrogate_key(["'-1'"]) }} AS product_key
    ,'-1' AS product_id
    ,'Unknown' AS product_name