{{ config(
    materialized = "table",
    tags=["dwh"]
) }}

SELECT
    sg.place_id,
    sg.name,
    sg.address,
    sg.lat,
    sg.lng,
    sg.google_maps_url,

    -- geographic point
    ST_GEOGPOINT(sg.lng, sg.lat) AS geom,

    -- first and last time the place appeared in the raw feed
    MIN(sg.fetched_at) AS first_seen_date,
    MAX(sg.fetched_at) AS last_seen_date

FROM {{ ref('staging_google_parking_places_dbt') }} AS sg

GROUP BY ALL
ORDER BY 1, 2
