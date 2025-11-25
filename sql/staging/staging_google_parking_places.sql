CREATE OR REPLACE TABLE
  `grand-water-473707-r8.staging.staging_google_parking_places` AS
SELECT
  place_id,
  name,
  address,
  lat,
  lng,
  ST_GEOGPOINT(lng, lat) AS geom,
  types,
  rating,
  user_ratings_total,
  fetched_at,

  -- demand score (basic)
  user_ratings_total
    + IFNULL(rating * 10, 0) AS demand_score

FROM `grand-water-473707-r8.raw.raw_google_charging_places`
WHERE LOWER(types) LIKE '%parking%';  -- filters parking-lot keywords
