CREATE OR REPLACE TABLE `grand-water-473707-r8.staging.staging_parking_fee_amsterdam` AS
WITH lvl1 AS (
  SELECT
    zone_id,
    SAFE.ST_GEOGFROMGEOJSON(location_json) AS zone_geography,
    description.__invalid_keys__[OFFSET(0)].value AS description,
    t AS tariff_struct
  FROM `grand-water-473707-r8.raw.raw_parking_fee_amsterdam`,
  UNNEST(tarieven) AS t
),

lvl2 AS (
  SELECT
    zone_id,
    zone_geography,
    description,

    -- Extract hourly rate (first-level key)
    t1.key AS hourly_rate_str,

    -- Extract second level struct (period + days)
    t1.value.__invalid_keys__[OFFSET(0)].key AS periode,
    t1.value.__invalid_keys__[OFFSET(0)].value AS days
  FROM lvl1,
  UNNEST(tariff_struct.__invalid_keys__) AS t1
)

SELECT
  zone_id,
  description,
  zone_geography,

  -- numeric price
  SAFE_CAST(REPLACE(hourly_rate_str, ',', '.') AS FLOAT64) AS hourly_rate,

  periode,
  days,

  -- centroid for join with ParkBee
  ST_Y(ST_CENTROID(zone_geography)) AS zone_lat,
  ST_X(ST_CENTROID(zone_geography)) AS zone_lng

FROM lvl2;
