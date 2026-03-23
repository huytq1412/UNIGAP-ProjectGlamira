WITH source AS (
    SELECT 
        product_id,
        product_name
    FROM {{ source('raw_layer_avro', 'product_names') }}
    WHERE product_id IS NOT NULL
)
SELECT 
    product_id,
    product_name
FROM source