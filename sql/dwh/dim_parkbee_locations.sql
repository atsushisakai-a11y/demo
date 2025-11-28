CREATE OR REPLACE TABLE
  `grand-water-473707-r8.dwh.dim_parkbee_locations` AS
WITH
  latest AS (
  SELECT
    spg.location_id,
    MAX(spg.scrape_datetime_cet) AS last_seen_datetime,
    MIN(spg.scrape_datetime_cet) AS first_seen_datetime
  FROM
    `grand-water-473707-r8.staging.staging_parkbee_garages` spg
  GROUP BY
    ALL )
SELECT
  spg.location_id,
  spg.country,
  spg.city,
  spg.name,
  spg.latitude,
  spg.longitude,
  ST_GEOGPOINT(spg.longitude, spg.latitude) AS geom,
  l.first_seen_datetime,
  l.last_seen_datetime
FROM
  `grand-water-473707-r8.staging.staging_parkbee_garages` spg
INNER JOIN
  latest l
ON
  l.location_id = spg.location_id
  AND l.last_seen_datetime = spg.scrape_datetime_cet
ORDER BY
  1,
  2,
  3
