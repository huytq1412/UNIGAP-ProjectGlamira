WITH dim_date__source__distinct_locations AS (
    -- Quét danh mục IP, chỉ giữ lại các tổ hợp địa lý độc nhất
    SELECT DISTINCT
        location_key,
        country_code,
        country_name,
        region_name,
        city_name
    FROM {{ ref('stg_ip_locations') }}
)
SELECT * FROM dim_date__source__distinct_locations
UNION ALL
-- Xử lý Default record cho các IP không map được
SELECT 
    {{ dbt_utils.generate_surrogate_key(["'Unknown'", "'Unknown'", "'Unknown'"]) }} AS location_key,
    'Unknown' AS country_code,
    'Unknown' AS country_name,
    'Unknown' AS region_name,
    'Unknown' AS city_name