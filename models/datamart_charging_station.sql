-- models/datamart_charging_station.sql
-- Purpose: final datamart combining dim + fact charging station data

{{ config(
    materialized='table'
) }}

SELECT
  dcs.search_keyword,
  COUNT(DISTINCT dcs.place_id) AS places,
  SUM(fcs.rating) AS total_rating,
  SUM(fcs.user_ratings_total) AS total_user_ratings,
  ROUND(
    SAFE_DIVIDE(
      SUM(fcs.rating),
      SUM(fcs.user_ratings_total)
    ), 1
  ) AS average_rating
FROM
  {{ ref('dim_charging_station') }} AS dcs
INNER JOIN
  {{ ref('fact_charging_station') }} AS fcs
ON
  fcs.place_id = dcs.place_id
GROUP BY
  dcs.search_keyword
ORDER BY
  1
