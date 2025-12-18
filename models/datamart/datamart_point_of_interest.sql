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


WITH parkbee_parking AS (
  SELECT
    fpl.parking_from_cet,
    name AS parking_name,
    latitude,
    longitude,
    ST_BUFFER(
      ST_GEOGPOINT(longitude, latitude),
      500
    ) AS buffer_500m,
    fpl.occupancy_rate
  FROM {{ ref('fact_parkbee_locations_dbt') }} fpl fpl
    INNER JOIN 
  WHERE scrape_date = '2025-12-18'
),

google_pois AS (
  SELECT
    place_id,
    name AS poi_name,
    location_type,
    geom,
    google_maps_url as url
  FROM {{ ref('dim_google_places_dbt') }} AS dgp
  WHERE location_type = 'office'
),

parkbee_with_poi AS (
  SELECT
    'parkbee' AS record_type,
    p.parking_from_cet AS reference_time,
    p.parking_name AS name,
    'parking' AS primary_type,
    'parking' AS location_type,
    p.latitude AS lat,
    p.longitude AS lng,
    ST_GEOGPOINT(p.longitude, p.latitude) AS geom,
    p.occupancy_rate,
    COUNT(g.place_id) AS poi_count_500m
  FROM parkbee_parking p
  LEFT JOIN google_pois g
    ON ST_CONTAINS(p.buffer_500m, g.geom)
  GROUP BY
    p.parking_from_cet,
    p.parking_name,
    p.latitude,
    p.longitude,
    p.occupancy_rate
)

-- 🔽 UNION ParkBee + Google POIs
SELECT
  record_type,
  name,
  location_type,
  lat,
  lng,
  geom,
  NULL as url,
  occupancy_rate,
  poi_count_500m
FROM parkbee_with_poi

UNION ALL

SELECT
  'google_poi' AS record_type,
  poi_name AS name,
  location_type,
  ST_Y(geom) AS lat,
  ST_X(geom) AS lng,
  geom,
  url,
  NULL AS occupancy_rate,
  NULL AS poi_count_500m
FROM google_pois;
