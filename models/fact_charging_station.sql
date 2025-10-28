{{ config(materialized='table') }}

SELECT
  place_id,
  rating,
  user_ratings_total
FROM {{ ref('staging_charging_station') }}
