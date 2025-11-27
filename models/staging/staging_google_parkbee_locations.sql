{{ config(
    materialized='table',
    description="Standardized Google Places → ParkBee-related locations."
) }}

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
FROM {{ ref('raw_google_charging_places') }} AS rg
