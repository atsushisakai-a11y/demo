CREATE OR REPLACE TABLE `grand-water-473707-r8.staging.staging_parking_fee_amsterdam` AS
WITH base AS (
  SELECT
    zone_id,

    -- Convert GeoJSON string → GEOGRAPHY
    SAFE.ST_GEOGFROMGEOJSON(location_json) AS zone_geography,

    -- Extract description from STRUCT
    description.__invalid_keys__[0].value AS description,

    -- tarieven is nested deeply; keep as struct
    tarieven AS tarieven_struct

  FROM `grand-water-473707-r8.raw.raw_parking_fee_amsterdam`
),

-- Level 1 UNNEST → tarieven[]
lvl1 AS (
  SELECT
    zone_id,
    zone_geography,
    description,
    t1.__invalid_keys__ AS lvl1_array
  FROM base,
  UNNEST(tarieven_struct) AS t1
),

-- Level 2 UNNEST → tarieven[].__invalid_keys[]
lvl2 AS (
  SELECT
    zone_id,
    zone_geography,
    description,
    lvl1_item.value.__invalid_keys__ AS lvl2_array
  FROM lvl1,
  UNNEST(lvl1_array) AS lvl1_item
),

-- Level 3 UNNEST → actual tariff JSON objects
lvl3 AS (
  SELECT
    zone_id,
    zone_geography,
    description,
    lvl2_item.value AS tariff_json_string
  FROM lvl2,
  UNNEST(lvl2_array) AS lvl2_item
),

-- Extract fields from tariff JSON
tariffs_final AS (
  SELECT
    zone_id,
    zone_geography,
    description,
    JSON_VALUE(tariff_json_string, '$.periode') AS periode,
    JSON_VALUE(tariff_json_string, '$.days') AS days,
    SAFE_CAST(JSON_VALUE(tariff_json_string, '$.tarief') AS FLOAT64) AS hourly_rate
  FROM lvl3
)

SELECT
  zone_id,
  description,
  zone_geography,

  -- Centroid for spatial joins
  ST_Y(ST_CENTROID(zone_geography)) AS zone_lat,
  ST_X(ST_CENTROID(zone_geography)) AS zone_lng,

  periode,
  days,
  hourly_rate

FROM tariffs_final;
