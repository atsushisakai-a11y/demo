  {{ config(
      materialized = "table",
      tags = ["datamart"]
  ) }}
WITH
  count_per_location AS (
  SELECT
    fpl.location_id,
    COUNT(*) AS counts
  FROM
    `grand-water-473707-r8.dwh.fact_parkbee_locations_dbt` fpl
  WHERE
    CAST(fpl.parking_from_cet AS date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY
    ALL
  HAVING
    COUNT(*) >= 8 )
SELECT
  dpl.country,
  dpl.city,
  dpl.name,
  dpl.location_id,
  dpl.lat,
  dpl.long,
  dpl.geom,
  CAST(fpl.parking_from_cet AS date) AS parking_date,
  fpl.parking_from_hour,
  fpl.parking_from_weekday,
  fpl.hourly_price,
  fpl.occupancy_rate
FROM
  `grand-water-473707-r8.dwh.fact_parkbee_locations_dbt` fpl
INNER JOIN
  `grand-water-473707-r8.dwh.dim_parkbee_locations_dbt` dpl
ON
  dpl.location_id = fpl.location_id
INNER JOIN
  count_per_location cpl
ON
  cpl.location_id = fpl.location_id
WHERE
  CAST(fpl.parking_from_cet AS date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY
  1,
  2,
  3,
  4
