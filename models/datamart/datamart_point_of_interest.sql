{{ config(
    materialized = "table",
    tags = ["datamart"]
) }}

SELECT
  dgp.name,
  dgp.primary_type,
  dgp.location_type,
  dgp.address,
  dgp.geom,
  dgp.lat,
  dgp.lng,
  dgp.google_maps_url,
  fpcl.place_id,
  fpcl.rating,
  fpcl.user_ratings_total
FROM {{ ref('fact_parking_candidate_locations_dbt') }} AS fpcl
INNER JOIN {{ ref('dim_google_places_dbt') }} AS dgp
  ON dgp.place_id = fpcl.place_id
