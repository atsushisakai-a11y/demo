CREATE OR REPLACE TABLE `grand-water-473707-r8.staging.staging_parkbee_locations_combined` AS
WITH matched_locations AS (
  SELECT
    spg.location_id,
    loc.place_id,
    spg.name AS name_parkbee,
    spg.country AS country_parkbee,
    spg.city AS city_parkbee,
    loc.name AS name_google,
    loc.address AS address_google,
    spg.price_cost,
    spg.price_currency,
    spg.available_spaces,
    spg.total_spaces,
    spg.scrape_datetime AS scrape_datetime_parkbee,
    loc.fetched_at AS scrape_datetime_google,
    spg.latitude AS parkbee_lat,
    spg.longitude AS parkbee_lng,
    loc.lat AS google_lat,
    loc.lng AS google_lng,
    loc.google_maps_url,
    ST_DISTANCE(
      ST_GEOGPOINT(spg.longitude, spg.latitude),
      ST_GEOGPOINT(loc.lng, loc.lat)
    ) AS distance_meters,
    loc.rating AS avg_rating,
    loc.user_ratings_total AS total_review
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
SELECT
  *,
  -- Convert UTC → Europe/Amsterdam
  DATETIME(TIMESTAMP(CURRENT_TIMESTAMP()), "Europe/Amsterdam") AS created_datetime
FROM matched_locations
--QUALIFY ROW_NUMBER() OVER (PARTITION BY location_id ORDER BY distance_meters ASC) = 1
  ;
