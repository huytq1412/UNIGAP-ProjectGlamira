WITH source AS (
  SELECT ip, country_short, country_long, region, city
  FROM {{ source('raw_layer_avro', 'ip_locations') }}
  WHERE ip IS NOT NULL
)
SELECT 
      ip AS ip_address,
      country_short AS country_code,
      country_long AS country_name,
      region AS region_name, 
      city AS city_name
FROM source