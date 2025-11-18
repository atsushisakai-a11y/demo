CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.dim_parkbee_locations` AS
SELECT
  l.id AS location_id,
  l.name AS location_name,
  l.country,
  l.city,
  l.latitude,
  l.longitude
FROM `grand-water-473707-r8.staging.staging_parkbee_garages` l
;
