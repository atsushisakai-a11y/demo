CREATE OR REPLACE TABLE `grand-water-473707-r8.staging.staging_parkbee_locations_combined` AS
SELECT
  spg.id AS location_id,
  loc.place_id,
  spg.name AS name_parkbee,
  loc.name AS name_google,
  loc.address AS address_google,
  spg.price_cost,
  spg.price_currency,
  spg.available_spaces,
  spg.total_spaces,
  spg.scrape_datetime,
  spg.latitude,
  spg.longitude,
  loc.lat,
  loc.lng,
  ST_DISTANCE( ST_GEOGPOINT(spg.longitude, spg.latitude), ST_GEOGPOINT(loc.lng, loc.lat) ) AS distance_meters
FROM
  `grand-water-473707-r8.staging.staging_parkbee_garages` spg
LEFT JOIN
  `grand-water-473707-r8.staging.staging_google_parkbee_locations` loc
ON
  ST_DISTANCE( ST_GEOGPOINT(spg.longitude, spg.latitude), ST_GEOGPOINT(loc.lng, loc.lat) ) < 300
