-- WITH fact_ips AS (
--     -- Lấy dữ liệu IP từ từ event thực
--     SELECT ip_address
--     FROM {{ ref('stg_raw_data') }}
--     WHERE ip_address IS NOT NULL
--     GROUP BY ip_address
-- ),
-- stg_ip_locations AS (
--     -- Lấy dữ liệu IP từ danh mục
--     SELECT 
--         ip_address
--         ,MAX(country_code) AS country_code
--         ,MAX(country_name) AS country_name
--         ,MAX(region_name) AS region_name
--         ,MAX(city_name) AS city_name
--     FROM {{ ref('stg_ip_locations') }}
--     WHERE ip_address IS NOT NULL
--     GROUP BY ip_address
-- )
-- SELECT 
--     {{ dbt_utils.generate_surrogate_key(['COALESCE(f.ip_address, d.ip_address)']) }} AS location_key   
--     ,CAST(COALESCE(f.ip_address, d.ip_address) AS STRING) AS ip_address  
--     -- Nếu code map được với danh mục thì lấy info, không map được thì gán nhãn Unknown
--     ,CAST(COALESCE(d.country_code, 'Unknown') AS STRING) AS country_code
--     ,CAST(COALESCE(d.country_name, 'Unknown') AS STRING) AS country_name
--     ,CAST(COALESCE(d.region_name, 'Unknown') AS STRING) AS region_name
--     ,CAST(COALESCE(d.city_name, 'Unknown') AS STRING) AS city_name
-- FROM fact_ips AS f
--     FULL OUTER JOIN stg_ip_locations AS d ON f.ip_address = d.ip_address

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