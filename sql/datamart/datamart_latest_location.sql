WITH
  latest AS (
  SELECT
    fpl.location_id,
    MAX(fpl.scrape_datetime) AS max_scrape_datetime
  FROM
    `grand-water-473707-r8.dwh.fact_parkbee_locations` fpl
  GROUP BY
    ALL )
SELECT
  CAST(DATE_TRUNC(fpl.scrape_datetime, day) AS date) scrape_date,
  dpl.name,
  dpl.location_id,
  dpl.city,
  dpl.country,
  dpl.latitude,
  dpl.longitude,
  fpl.occupancy_rate,
  fpl.price_cost,
  fpl.occupancy,
  fpl.available_spaces,
  fpl.total_spaces
FROM
  `grand-water-473707-r8.dwh.fact_parkbee_locations` fpl
INNER JOIN
  latest l
ON
  l.location_id = fpl.location_id
  AND l.max_scrape_datetime = fpl.scrape_datetime
INNER JOIN
  `grand-water-473707-r8.dwh.dim_parkbee_locations` dpl
ON
  dpl.location_id = fpl.location_id
ORDER BY
  1,
  2
