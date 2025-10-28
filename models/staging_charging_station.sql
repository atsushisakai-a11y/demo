{{ config(materialized='view') }}

WITH latest AS (
  SELECT
    gcp.place_id,
    MAX(gcp.fetched_at) AS max_fetched_at
  FROM
    {{ source('osm_demo', 'google_charging_places') }} gcp
  GROUP BY gcp.place_id
)

SELECT
  gcp.place_id,
  CAST(DATE_TRUNC(gcp.fetched_at, day) AS date) AS fetched_at,
  gcp.name,
  CASE
    WHEN LOWER(gcp.name) LIKE '%charging%station%'
      THEN SPLIT(gcp.name, ' ')[OFFSET(0)]
    ELSE NULL
  END AS brand,
  gcp.address,
  gcp.lat,
  gcp.lng,
  gcp.google_maps_url,
  gcp.search_keyword,
  gcp.search_radius_m,
  gcp.types,
  gcp.rating,
  gcp.user_ratings_total
FROM {{ source('osm_demo', 'google_charging_places') }} gcp
JOIN latest l
  ON l.place_id = gcp.place_id
 AND l.max_fetched_at = gcp.fetched_at
