CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.datamart_price_comparison` AS
WITH
  latest AS (
  SELECT
    fpl.location_id,
    MAX(fpl.scrape_datetime_cet) AS max_scrape_datetime_cet
  FROM
    `grand-water-473707-r8.dwh.fact_parkbee_locations` fpl
  GROUP BY
    ALL )
SELECT
  CAST(DATE_TRUNC(fpl.scrape_datetime_cet, day) AS date) scrape_date,
  fpl.parking_from_cet,
  fpl.parking_to_cet,
  dpl.name,
  dpl.location_id,
  dpl.city,
  dpl.country,
  dpl.latitude,
  dpl.longitude,
  fpl.occupancy_rate,
  fpl.price_cost,
  fpl.hourly_price,
  fpl.available_spaces,
  fpl.total_spaces,
  z.zone_id,
  z.hourly_rate AS public_hourly_price,
  (fpl.hourly_price - z.hourly_rate) AS price_gap,
  CASE
    WHEN fpl.hourly_price > z.hourly_rate THEN 'Public Cheaper'
    WHEN fpl.hourly_price < z.hourly_rate THEN 'ParkBee Cheaper'
    ELSE 'Same'
END
  AS price_position
FROM
  `grand-water-473707-r8.dwh.fact_parkbee_locations` fpl
INNER JOIN
  latest l
ON
  l.location_id = fpl.location_id
  AND l.max_scrape_datetime_cet = fpl.scrape_datetime_cet
INNER JOIN
  `grand-water-473707-r8.dwh.dim_parkbee_locations` dpl
ON
  dpl.location_id = fpl.location_id
LEFT JOIN
  `grand-water-473707-r8.dwh.fact_parking_fee_amsterdam` z
ON
  ST_WITHIN(fpl.geom, z.geom)
WHERE
  dpl.country = 'NL'
ORDER BY
  1,
  2
