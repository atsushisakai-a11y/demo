CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.fact_google_places` AS
SELECT
  rg.place_id,
  rg.name,
  rg.address,
  rg.lat,
  rg.lng,
  ST_GEOGPOINT(rg.lng, rg.lat) AS geom,
  rg.rating,
  rg.user_ratings_total,
  rg.google_maps_url,
  rg.fetched_at
FROM
  `grand-water-473707-r8.raw.raw_google_charging_places` rg
;
