WITH distinct_locations AS (
    -- Quét danh mục IP, chỉ giữ lại các tổ hợp địa lý độc nhất
    SELECT DISTINCT
        COALESCE(country_code, 'Unknown') AS country_code
        ,COALESCE(country_name, 'Unknown') AS country_name
        ,COALESCE(region_name, 'Unknown') AS region_name
        ,COALESCE(city_name, 'Unknown') AS city_name
    FROM {{ ref('stg_ip_locations') }}
    WHERE country_code IS NOT NULL
)

SELECT 
    -- Surrogate key dựa trên thông tin địa lý
    {{ dbt_utils.generate_surrogate_key(['country_code', 'region_name', 'city_name']) }} AS location_key   
    ,CAST(country_code AS STRING) AS country_code
    ,CAST(country_name AS STRING) AS country_name
    ,CAST(region_name AS STRING) AS region_name
    ,CAST(city_name AS STRING) AS city_name
FROM distinct_locations
UNION ALL
-- Xử lý Default record cho các IP không map được
SELECT 
    {{ dbt_utils.generate_surrogate_key(["'Unknown'", "'Unknown'", "'Unknown'"]) }} AS location_key,
    'Unknown' AS country_code,
    'Unknown' AS country_name,
    'Unknown' AS region_name,
    'Unknown' AS city_name