CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.dim_parkbee_locations` AS
SELECT
  DISTINCT spg.location_id,
  spg.country,
  spg.city,
  spg.name,
  spg.latitude,
  spg.longitude,
  ST_GEOGPOINT(spg.longitude, spg.latitude) AS geom
FROM
  `grand-water-473707-r8.staging.staging_parkbee_garages` spg
ORDER BY
  1,
  2,
  3
