{{ config(
    materialized = "table"
) }}

SELECT
  location_id,
  scrape_datetime_cet,
  parking_from_cet,
  parking_from_weekday,
  total_spaces,
  available_spaces,
  CASE
    WHEN total_spaces > 0 THEN (total_spaces - available_spaces) / total_spaces
    ELSE NULL
END
  AS utilization_rate,
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
  CASE
    WHEN total_spaces > 0 THEN (total_spaces - available_spaces) / total_spaces
    ELSE NULL
END
  AS utilization_rate,    
  NULL as hourly_price
FROM {{ ref('staging_google_parking_places') }}
