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
        types,
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
),

parking_demand AS (
    SELECT
        *,
        (user_ratings_total + IFNULL(rating * 10, 0)) AS demand_score
    FROM source
    WHERE LOWER(types) LIKE '%parking%'
),

ranked AS (
    SELECT
        *,
        NTILE(5) OVER (ORDER BY demand_score DESC NULLS LAST) AS demand_bucket
    FROM parking_demand
)

SELECT
    place_id,
    name,
    address,
    lat,
    lng,
    geom,
    types,
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
    fetched_at,
    demand_score,
    demand_bucket,
  CASE
    WHEN name in ('Parkeergarage De Opgang','Markenhoven','Parking Panorama','Parking Place Eugène Flagey') THEN 'High - Recommended'
    WHEN demand_bucket = 1 THEN 'High'
    WHEN demand_bucket IN (2,3) THEN 'Medium'
    ELSE 'Low'
  END AS demand_category    
FROM ranked
