{{ config(
    materialized = "table",
    tags = ["dwh"]
) }}

SELECT
    location_id,
    rating,
    user_ratings_total
FROM
    {{ ref('staging_google_parking_places') }}
