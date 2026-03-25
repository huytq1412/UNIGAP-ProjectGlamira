WITH stg_ip_locations__source AS (
  SELECT ip, country_short, country_long, region, city
  FROM {{ source('raw_layer_avro', 'ip_locations') }}
  WHERE ip IS NOT NULL
),
stg_ip_locations__cleaned AS (
  -- Xử lý luôn các giá trị NULL thành 'Unknown' tại đây
  SELECT 
      CAST(ip AS STRING) AS ip_address,
      CAST(COALESCE(country_short, 'Unknown') AS STRING) AS country_code,
      CAST(COALESCE(country_long, 'Unknown') AS STRING) AS country_name,
      CAST(COALESCE(region, 'Unknown') AS STRING) AS region_name, 
      CAST(COALESCE(city, 'Unknown') AS STRING) AS city_name
  FROM stg_ip_locations__source
)
SELECT 
    ip_address,
    country_code,
    country_name,
    region_name,
    city_name,
    -- Tạo location_key ngay tại staging
    {{ dbt_utils.generate_surrogate_key(['country_code', 'region_name', 'city_name']) }} AS location_key
FROM stg_ip_locations__cleaned