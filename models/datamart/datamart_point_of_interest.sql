{{ config(
    materialized = "table",
    tags = ["datamart"]
) }}

WITH parkbee_parking AS (
  SELECT
    fl.parking_from_cet,
    dl.name AS parking_name,
    dl.latitude,
    dl.longitude,
    ST_BUFFER(
      ST_GEOGPOINT(dl.longitude, dl.latitude),
      500
    ) AS buffer_500m,
    fl.utilization_rate
  FROM {{ ref('fact_location') }} fl
  INNER JOIN {{ ref('dim_location') }} dl
    ON dl.location_id = fl.location_id
  WHERE date_trunc(fl.scrape_datetime_cet, day) = '2025-12-18'
    AND dl.platform = 'parkbee'
),

google_pois AS (
  SELECT
    location_id,
    name AS poi_name,
    location_type,
    geom,
    url
  FROM {{ ref('dim_location') }} AS dl
  WHERE location_type in ('office','parking')
    AND platform = 'google'
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
