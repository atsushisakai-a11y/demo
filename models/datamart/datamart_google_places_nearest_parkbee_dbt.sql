{{ config(
    materialized = "table",
    tags = ["datamart"]
) }}

-- ======================================
-- 1. Combine Google dim + fact
-- ======================================
WITH dim_fact_google AS (
    SELECT
        dgp.name,
        dgp.address,
        dgp.geom,
        dgp.lat,
        dgp.lng,
        dgp.google_maps_url,
        fpcl.place_id,
        fpcl.rating,
        fpcl.user_ratings_total,
        fpcl.demand_score,
        fpcl.demand_category
    FROM {{ ref('fact_parking_candidate_locations_dbt') }} AS fpcl
    INNER JOIN {{ ref('dim_google_places') }} AS dgp
        ON dgp.place_id = fpcl.place_id
),

-- ======================================
-- 2. Join ParkBee + compute distance
-- ======================================
join_parkbee AS (
    SELECT
        dfg.place_id,
        dfg.name AS google_place_name,
        dfg.address AS google_place_address,
        dfg.lat AS google_lat,
        dfg.lng AS google_lng,
        dfg.google_maps_url,
        dfg.geom AS google_geom,
        dfg.rating AS google_rating,
        dfg.user_ratings_total AS google_reviews,
        dfg.demand_score,
        dfg.demand_category,

        dpl.location_id AS parkbee_location_id,
        dpl.name AS parkbee_name,
        dpl.city AS parkbee_city,
        dpl.country AS parkbee_country,
        dpl.latitude AS parkbee_lat,
        dpl.longitude AS parkbee_lng,
        dpl.geom AS parkbee_geom,

        -- ⭐ Compute distance
        ST_DISTANCE(dfg.geom, dpl.geom) AS distance_meters,

        -- Row numbering → keep NEAREST ParkBee only
        ROW_NUMBER() OVER (
            PARTITION BY dfg.place_id
            ORDER BY ST_DISTANCE(dfg.geom, dpl.geom)
        ) AS rn

    FROM dim_fact_google dfg
    CROSS JOIN {{ ref('dim_parkbee_locations_dbt') }} AS dpl
)

-- ======================================
-- 3. Select only nearest ParkBee + enrich
-- ======================================
SELECT
    place_id,
    google_place_name,
    google_place_address,
    google_lat,
    google_lng,
    google_maps_url,
    google_rating,
    google_reviews,
    demand_score,
    demand_category,

    parkbee_location_id,
    parkbee_name,
    parkbee_city,
    parkbee_country,
    parkbee_lat,
    parkbee_lng,

    distance_meters,
    distance_meters / 1000 AS distance_km,

    CASE
        WHEN distance_meters <= 100 THEN '0–100m (Very Close)'
        WHEN distance_meters <= 300 THEN '100–300m (Close)'
        WHEN distance_meters <= 500 THEN '300–500m (Walkable)'
        WHEN distance_meters <= 1000 THEN '0.5–1 km (Slightly Far)'
        ELSE '> 1 km (Far)'
    END AS distance_category,

    CASE
        WHEN LOWER(google_place_name) LIKE '%parkbee%' THEN 'Yes'
        ELSE 'No'
    END AS on_parkbee

FROM join_parkbee
WHERE rn = 1

ORDER BY distance_meters ASC;
