CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.dim_parkbee_locations` AS
SELECT
distinct
  location_id,
  place_id,
  country_parkbee as country,
  city_parkbee as city,
  name_parkbee,
  name_google,
  parkbee_lat,
  parkbee_lng,
  google_maps_url
FROM
  `grand-water-473707-r8.staging.staging_parkbee_locations_combined` l
order by 1,2,3
