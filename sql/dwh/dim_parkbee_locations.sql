CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.dim_parkbee_locations` AS
SELECT
  DISTINCT location_id,
  country,
  city,
  name,
  latitude,
  longitude,
  ST_GEOGPOINT(spg.longitude, spg.latitude) AS geom
FROM
  `grand-water-473707-r8.staging.staging_parkbee_garages` l
ORDER BY
  1,
  2,
  3
