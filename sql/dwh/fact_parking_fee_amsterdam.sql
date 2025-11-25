CREATE OR REPLACE TABLE
  `grand-water-473707-r8.dwh.fact_parking_fee_amsterdam` AS
SELECT
  spfa.zone_id,
  spfa.days,
  spfa.description,
  spfa.hourly_rate,
  spfa.periode,
  spfa.zone_geography,
  spfa.zone_lat,
  spfa.zone_lng,
  ST_GEOGPOINT(spfa.zone_lng, spfa.zone_lat) AS geom,
FROM
  `grand-water-473707-r8.staging.staging_parking_fee_amsterdam` spfa
ORDER BY
  1,
  2
