WITH fact_ips AS (
    -- Lấy dữ liệu IP từ từ event thực
    SELECT ip_address
    FROM {{ ref('stg_raw_data') }}
    WHERE ip_address IS NOT NULL
    GROUP BY ip_address
),
stg_ip_locations AS (
    -- Lấy dữ liệu IP từ danh mục
    SELECT 
        ip_address
        ,MAX(country_code) AS country_code
        ,MAX(country_name) AS country_name
        ,MAX(region_name) AS region_name
        ,MAX(city_name) AS city_name
    FROM {{ ref('stg_ip_locations') }}
    WHERE ip_address IS NOT NULL
    GROUP BY ip_address
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['COALESCE(f.ip_address, d.ip_address)']) }} AS location_key   
    ,CAST(COALESCE(f.ip_address, d.ip_address) AS STRING) AS ip_address  
    -- Nếu code map được với danh mục thì lấy info, không map được thì gán nhãn Unknown
    ,CAST(COALESCE(d.country_code, 'Unknown') AS STRING) AS country_code
    ,CAST(COALESCE(d.country_name, 'Unknown') AS STRING) AS country_name
    ,CAST(COALESCE(d.region_name, 'Unknown') AS STRING) AS region_name
    ,CAST(COALESCE(d.city_name, 'Unknown') AS STRING) AS city_name
FROM fact_ips AS f
    FULL OUTER JOIN stg_ip_locations AS d ON f.ip_address = d.ip_address
