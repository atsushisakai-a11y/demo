CREATE OR REPLACE TABLE
  `grand-water-473707-r8.staging.staging_google_parking_places` AS
with demand_score as (
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
  google_maps_url,
  user_ratings_total * rating AS demand_score

FROM `grand-water-473707-r8.raw.raw_google_charging_places`
WHERE LOWER(types) LIKE '%parking%'
  ),
 ranked AS (
  SELECT
    ds.*,
    NTILE(5) OVER (ORDER BY ds.demand_score DESC) AS demand_bucket
  FROM demand_score ds
)
SELECT
  *,
  CASE
    WHEN name in ('Parkeergarage De Opgang','Markenhoven','Parking Panorama','Parking Place Eugène Flagey') THEN 'High - Recommended'
    WHEN demand_bucket = 1 THEN 'High'
    WHEN demand_bucket IN (2,3) THEN 'Medium'
    ELSE 'Low'
  END AS demand_category
FROM ranked;
