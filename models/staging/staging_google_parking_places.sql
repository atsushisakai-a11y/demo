{{ config(
    materialized = "table",
    alias = "staging_google_parking_places"
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
    types,
    CASE
       WHEN LOWER(name) LIKE '%parkbee%' THEN 'parking_parkbee'
        WHEN primary_type IN ('parking') THEN 'parking'
        WHEN LOWER(name) LIKE '%charging%station%'
          OR LOWER(name) LIKE '%recharge%'
          OR primary_type IN ('oplaadpunt')
          THEN 'charging station'
        WHEN primary_type LIKE '%store%' THEN 'store'
        WHEN
            primary_type LIKE '%office%'
            OR primary_type LIKE '%company%'
            OR primary_type IN (
                'real_estate_agency', 'plumber', 'accounting',
                'finance', 'bank', 'lawyer','general_contractor','insurance_agency'
            )
            OR types like '%finance%'
            OR primary_type = 'point_of_interest' 
            THEN 'office'

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
QUALIFY
    ROW_NUMBER() OVER (
        PARTITION BY place_id
        ORDER BY fetched_at DESC
    ) = 1
