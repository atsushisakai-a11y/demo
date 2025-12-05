{{ config(
    materialized = "table",
    tags = ["dwh"]
) }}

SELECT
    place_id,
    rating,
    user_ratings_total,
    demand_score,
    demand_category
FROM
    {{ ref('staging_google_parking_places_dbt') }};
