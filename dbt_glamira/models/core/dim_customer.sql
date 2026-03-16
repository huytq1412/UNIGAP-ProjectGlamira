WITH stg_raw AS
(
    SELECT
        user_id
        ,device_id
        ,email_address
    FROM {{ ref('stg_raw_data') }}
    WHERE user_id IS NOT NULL OR device_id IS NOT NULL
)
SELECT
    -- Tạo Surrogate Key bằng dbt_utils
    {{ dbt_utils.generate_surrogate_key(['user_id', 'device_id']) }} AS customer_key
    ,CAST(user_id AS STRING) AS user_id
    ,CAST(device_id AS STRING) AS device_id
    ,CAST(MAX(email_address) AS STRING) AS email_address
FROM stg_raw
GROUP BY user_id, device_id