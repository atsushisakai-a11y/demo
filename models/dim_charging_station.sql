{{ config(materialized='table') }}

SELECT
  place_id,
  fetched_at,
  name,
  brand,
  address,
  lat,
  lng,
  google_maps_url,
  search_keyword,
  types
FROM {{ ref('staging_charging_station') }}
