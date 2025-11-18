CREATE OR REPLACE TABLE `grand-water-473707-r8.staging.staging_raw_google_parkbee_locations` AS
SELECT
  rpl.place_id,
  rpl.name,
  rpl.address,
  rpl.lat,
  rpl.lng,
  rpl.rating,
  rpl.user_ratings_total,
  rpl.google_maps_url,
  rpl.fetched_at
FROM
  `grand-water-473707-r8.raw.raw_google_parkbee_locations` rpl
;
