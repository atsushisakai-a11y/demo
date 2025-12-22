  {{ config(
      materialized = "table",
      tags = ["datamart"]
  ) }}
WITH
  count_per_location AS (
  SELECT
    fl.location_id,
    COUNT(*) AS counts
  FROM
    {{ ref('fact_location') }} fl
  WHERE
    CAST(fl.parking_from_cet AS date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY
    ALL
  HAVING
    COUNT(*) >= 8 )
SELECT
  dl.country,
  dl.city,
  dl.name,
  dl.location_id,
  dl.latitude,
  dl.longitude,
  dl.geom,
  fl.parking_from_cet,
  CAST(fl.parking_from_cet AS date) AS parking_date,
  fl.parking_from_weekday,
  fl.hourly_price,
  fl.utilization_rate
FROM
  {{ ref('fact_location') }} fl
INNER JOIN
  {{ ref('dim_location') }} dl
ON
  dl.location_id = fl.location_id
INNER JOIN
  count_per_location cpl
ON
  cpl.location_id = fl.location_id
WHERE
  CAST(fl.parking_from_cet AS date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    and dl.platform = 'parkbee'
ORDER BY
  1,
  2,
  3,
  4
