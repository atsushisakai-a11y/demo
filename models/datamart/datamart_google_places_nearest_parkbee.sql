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
        dgp.primary_type,
        dgp.address,
        dgp.geom,
        dgp.lat,
        dgp.lng,
        dgp.google_maps_url,
        fpcl.location_id,
        fpcl.rating,
        fpcl.user_ratings_total
    FROM {{ ref('fact_parking_candidate_locations') }} AS fpcl
    INNER JOIN {{ ref('dim_google_places') }} AS dgp
        ON dgp.location_id = fpcl.location_id
),
parking_demand AS (
    SELECT
        *,
        (user_ratings_total + IFNULL(rating * 10, 0)) AS demand_score
    FROM dim_fact_google
    WHERE LOWER(primary_type) LIKE '%parking%'
),

ranked AS (
    SELECT
        *,
        NTILE(3) OVER (ORDER BY demand_score DESC NULLS LAST) AS demand_bucket
    FROM parking_demand
),
-- ======================================
-- 2. Join ParkBee + compute distance
-- ======================================
join_parkbee AS (
    SELECT
        dfg.location_id,
        dfg.name AS google_place_name,
        dfg.primary_type,
        dfg.address AS google_place_address,
        dfg.lat AS google_lat,
        dfg.lng AS google_lng,
        dfg.google_maps_url,
        dfg.geom AS google_geom,
        dfg.rating AS google_rating,
        dfg.user_ratings_total AS google_reviews,
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
            PARTITION BY dfg.location_id
            ORDER BY ST_DISTANCE(dfg.geom, dpl.geom)
        ) AS rn

    FROM dim_fact_google dfg
    CROSS JOIN {{ ref('dim_parkbee_locations') }} AS dpl
)

-- ======================================
-- 3. Select only nearest ParkBee + enrich
-- ======================================
SELECT
    jp.location_id,
    jp.google_place_name,
    jp.google_place_address,
    jp.primary_type,
    jp.google_lat,
    jp.google_lng,
    jp.google_maps_url,
    jp.google_rating,
    jp.google_reviews,
    r.demand_score,
  CASE
    WHEN r.name in ('Parkeergarage De Opgang','Markenhoven','Parking Panorama','Parking Place Eugène Flagey') THEN 'High - Recommended'
    WHEN r.demand_bucket = 1 THEN 'High'
    WHEN r.demand_bucket = 2 THEN 'Medium'
    ELSE 'Low'
  END AS demand_category,
    
    jp.parkbee_location_id,
    jp.parkbee_name,
    jp.parkbee_city,
    jp.parkbee_country,
    jp.parkbee_lat,
    jp.parkbee_lng,

    jp.distance_meters,
    jp.distance_meters / 1000 AS distance_km,

    CASE
        WHEN jp.distance_meters <= 100 THEN '0–100m (Very Close)'
        WHEN jp.distance_meters <= 300 THEN '100–300m (Close)'
        WHEN jp.distance_meters <= 500 THEN '300–500m (Walkable)'
        WHEN jp.distance_meters <= 1000 THEN '0.5–1 km (Slightly Far)'
        ELSE '> 1 km (Far)'
    END AS distance_category,

    CASE
        WHEN LOWER(jp.google_place_name) LIKE '%parkbee%' THEN 'Yes'
        ELSE 'No'
    END AS on_parkbee

FROM join_parkbee jp
LEFT JOIN ranked r
    ON r.location_id = jp.location_id
WHERE jp.rn = 1
AND LOWER(jp.primary_type) LIKE '%parking%'
ORDER BY jp.distance_meters ASC
