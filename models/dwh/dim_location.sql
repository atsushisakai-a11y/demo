{{ config(
    materialized = "table",
    tags=["dwh"]
) }}

WITH
  latest AS (
    SELECT
      spg.location_id,
      MAX(spg.scrape_datetime_cet) AS last_seen_datetime,
      MIN(spg.scrape_datetime_cet) AS first_seen_datetime
    FROM {{ ref('staging_parkbee_garages') }} spg
    GROUP BY spg.location_id
  )
SELECT
  spg.platform,
  spg.location_id,
  spg.location_id AS external_id,
  spg.country,
  spg.city,
  spg.name,
  NULL AS address,
  NULL AS primary_type,
  'parking' AS location_type,
  spg.latitude,
  spg.longitude,
  NULL AS url,
  ST_GEOGPOINT(spg.longitude, spg.latitude) AS geom,
  DATE_TRUNC(l.first_seen_datetime, DAY) AS first_seen_date,
  DATE_TRUNC(l.last_seen_datetime, DAY) AS last_seen_date
FROM {{ ref('staging_parkbee_garages') }} spg
INNER JOIN latest l
  ON
    l.location_id = spg.location_id
    AND l.last_seen_datetime
      = spg.scrape_datetime_cet
UNION ALL
SELECT
  sg.platform,
  sg.location_id,
  sg.location_id AS external_id,
  NULL AS country,
  NULL AS city,
  sg.name,
  sg.address,
  sg.primary_type,
  sg.location_type,
  sg.lat,
  sg.lng,
  sg.google_maps_url,
  ST_GEOGPOINT(sg.lng, sg.lat) AS geom,
  MIN(sg.fetched_at) AS first_seen_date,
  MAX(sg.fetched_at) AS last_seen_date
FROM {{ ref('staging_google_parking_places') }} AS sg
GROUP BY ALL
ORDER BY 1, 2
