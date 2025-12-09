  {{ config(
      materialized = "table",
      tags = ["datamart"]
  ) }}
WITH
  count_per_location AS (
  SELECT
    fpl.location_id,
    avg(fpl.hourly_price) as avg_hourly_price,
    COUNT(*) AS counts
  FROM
    {{ ref('fact_parkbee_locations_dbt') }} fpl
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
  dpl.latitude,
  dpl.longitude,
  dpl.geom,
  avg(fpl.hourly_price) as avg_hourly_price,
  avg(fpl.hourly_price - cpl.avg_hourly_price) mad_hourly_price,
  max(fpl.hourly_price) as max_hourly_price,
  min(fpl.hourly_price) as min_hourly_price,
FROM
  {{ ref('fact_parkbee_locations_dbt') }} fpl
INNER JOIN
  {{ ref('dim_parkbee_locations_dbt') }} dpl
ON
  dpl.location_id = fpl.location_id
INNER JOIN
  count_per_location cpl
ON
  cpl.location_id = fpl.location_id
WHERE
  CAST(fpl.parking_from_cet AS date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
group by all
ORDER BY
  1,
  2,
  3,
  4
