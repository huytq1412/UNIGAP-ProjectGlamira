WITH fact_products AS (
    -- Lấy danh sách product_id từ event thực
    SELECT product_id
    FROM {{ ref('stg_raw_data') }}
    WHERE product_id IS NOT NULL
    GROUP BY product_id
),
stg_product_names AS (
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
FROM fact_products AS f
    FULL OUTER JOIN stg_product_names AS d ON f.product_id = d.product_id

