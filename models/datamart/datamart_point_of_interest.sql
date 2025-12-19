{{ config(
    materialized = "table",
    tags = ["datamart"]
) }}

WITH parkbee_parking AS (
  SELECT
    fpl.parking_from_cet,
    dpl.name AS parking_name,
    dpl.latitude,
    dpl.longitude,
    ST_BUFFER(
      ST_GEOGPOINT(dpl.longitude, dpl.latitude),
      500
    ) AS buffer_500m,
    fpl.utilization_rate
  FROM {{ ref('fact_parkbee_locations') }} fpl
  INNER JOIN {{ ref('dim_parkbee_locations') }} dpl
    ON dpl.location_id = fpl.location_id
  WHERE date_trunc(fpl.scrape_datetime_cet, day) = '2025-12-18'
),

google_pois AS (
  SELECT
    location_id,
    name AS poi_name,
    location_type,
    geom,
    google_maps_url as url
  FROM {{ ref('dim_google_places') }} AS dgp
  WHERE location_type in ('office','parking')
),

parkbee_with_poi AS (
  SELECT
    'parkbee' AS record_type,
    p.parking_from_cet AS reference_time,
    p.parking_name AS name,
    'parking' AS primary_type,
    'park bee' AS location_type,
    p.latitude AS lat,
    p.longitude AS lng,
    ST_GEOGPOINT(p.longitude, p.latitude) AS geom,
    p.utilization_rate,
    COUNT(g.location_id) AS poi_count_500m
  FROM parkbee_parking p
  LEFT JOIN google_pois g
    ON ST_CONTAINS(p.buffer_500m, g.geom)
  GROUP BY
    ALL
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
  utilization_rate,
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
FROM google_pois
