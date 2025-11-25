CREATE OR REPLACE TABLE
  `grand-water-473707-r8.dwh.fact_parking_candidate_locations` AS
SELECT
  place_id,
  rating,
  user_ratings_total,
  demand_score,
  demand_category
FROM
  `grand-water-473707-r8.staging.staging_google_parking_places`
