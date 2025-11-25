CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.dim_parkbee_locations` AS
SELECT
  DISTINCT spg.location_id,
  spg.country,
  spg.city,
  spg.name,
  spg.latitude,
  spg.longitude
FROM
  `grand-water-473707-r8.staging.staging_parkbee_garages` spg
  )
SELECT
  d.location_id,
  d.country,
  d.city,
  d.name,
  d.latitude,
  d.longitude,
  ST_GEOGPOINT(d.longitude, d.latitude) AS geom
FROM
  distincts d
ORDER BY
  1,
  2,
  3
