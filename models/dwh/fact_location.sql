SELECT
  location_id,
  scrape_datetime_cet,
  parking_from_cet,
  parking_from_weekday,
  total_spaces,
  available_spaces,
  hourly_price
FROM {{ ref('staging_parkbee_garages') }}

union all

SELECT
  location_id,
  fetched_at,
  NULL as parking_from_cet,
  NULL as parking_from_weekday,
  total_count,
  available_count,
  NULL as hourly_price
FROM {{ ref('staging_google_parking_places') }}
