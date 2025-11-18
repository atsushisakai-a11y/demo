CREATE OR REPLACE TABLE `grand-water-473707-r8.staging.staging_parkbee_locations_combined` AS
WITH matched_locations AS (
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
    spg.latitude AS parkbee_lat,
    spg.longitude AS parkbee_lng,
    loc.lat AS google_lat,
    loc.lng AS google_lng,
    ST_DISTANCE(
      ST_GEOGPOINT(spg.longitude, spg.latitude),
      ST_GEOGPOINT(loc.lng, loc.lat)
    ) AS distance_meters
  FROM
    `grand-water-473707-r8.staging.staging_parkbee_garages` spg
  LEFT JOIN
    `grand-water-473707-r8.staging.staging_google_parkbee_locations` loc
  ON
    ST_DISTANCE(
      ST_GEOGPOINT(spg.longitude, spg.latitude),
      ST_GEOGPOINT(loc.lng, loc.lat)
    ) < 300
)
SELECT * FROM matched_locations
-- Optionally, keep only closest match per ParkBee location
QUALIFY ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY distance_meters ASC) = 1;
