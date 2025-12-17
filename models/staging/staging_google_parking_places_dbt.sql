{{ config(
    materialized = "table",
    alias = "staging_google_parking_places_dbt"
) }}

WITH source AS (
    SELECT
        place_id,
        name,
        address,
        lat,
        lng,
        ST_GEOGPOINT(lng, lat) AS geom,
        SPLIT(types, ',')[SAFE_OFFSET(0)] AS primary_type,
        rating,
        user_ratings_total,
        google_maps_url,
        search_keyword,
        search_radius_m,
        connector_type,
        power_kw,
        available_count,
        total_count,
        charging_info_raw,
        is_parking,
        parking_address,
        parking_summary,
        parking_types_raw,
        fetched_at
    FROM {{ source('raw', 'raw_google_charging_places') }}
)

SELECT
    place_id,
    name,
    address,
    lat,
    lng,
    geom,
    primary_type,
    rating,
    user_ratings_total,
    google_maps_url,
    search_keyword,
    search_radius_m,
    connector_type,
    power_kw,
    available_count,
    total_count,
    charging_info_raw,
    is_parking,
    parking_address,
    parking_summary,
    parking_types_raw,
    fetched_at
    /*
    demand_score,
    demand_bucket,
  CASE
    WHEN name in ('Parkeergarage De Opgang','Markenhoven','Parking Panorama','Parking Place Eugène Flagey') THEN 'High - Recommended'
    WHEN demand_bucket = 1 THEN 'High'
    WHEN demand_bucket = 2 THEN 'Medium'
    ELSE 'Low'
  END AS demand_category
*/
FROM ranked
