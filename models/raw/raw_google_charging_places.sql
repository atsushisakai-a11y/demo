{{ config(
    materialized='table',
    description="Raw Google Places results for EV charging stations and parking facilities."
) }}

SELECT
  place_id,
  name,
  address,
  lat,
  lng,
  types,
  rating,
  user_ratings_total,
  google_maps_url,
  search_keyword,
  search_radius_m,

  -- EV-specific fields
  connector_type,
  power_kw,
  available_count,
  total_count,
  charging_info_raw,

  -- Parking-specific fields
  is_parking,
  parking_address,
  parking_summary,
  parking_types_raw,

  fetched_at

FROM {{ source('raw', 'raw_google_charging_places') }}
