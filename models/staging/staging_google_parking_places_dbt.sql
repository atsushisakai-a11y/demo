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
  CASE
    WHEN primary_type in ('parking') then 'parking'
    WHEN lower(name) LIKE '%charging%station%' or lower(name) LIKE '%recharge%' or primary_type in ('oplaadpunt') THEN 'charging station'
    WHEN
      primary_type LIKE '%office%'
      OR primary_type LIKE '%company%'
      OR primary_type IN (
        'real_estate_agency', 'plumber', 'accounting',
        'finance', 'bank', 'lawyer')
      THEN 'office'
    WHEN primary_type LIKE '%store%' THEN 'store'    
    ELSE 'other'
    END AS location_type,
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
    parking_address,
    parking_summary,
    parking_types_raw,
    fetched_at
FROM source
