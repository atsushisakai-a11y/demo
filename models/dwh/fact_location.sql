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
  hourly_price,
    NULL as avg_rating,
    NULL as ratings
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
    WHEN total_count > 0 THEN (total_count - available_count) / total_count
    ELSE NULL
END
  AS utilization_rate,    
  NULL as hourly_price,
  rating as avg_rating,
  user_ratings_total as ratings
FROM {{ ref('staging_google_parking_places') }}
