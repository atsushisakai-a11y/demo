WITH
  count_per_location AS (
    SELECT
      fl.location_id,
      avg(fl.hourly_price) AS avg_hourly_price,
      COUNT(*) AS counts
    FROM
      {{ ref('fact_location') }} fl
    WHERE
      CAST(fl.parking_from_cet AS date)
      >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY
      ALL
    HAVING
      COUNT(*) >= 8
  )
SELECT
  dl.country,
  dl.city,
  dl.name,
  dl.location_id,
  dl.latitude,
  dl.longitude,
  avg(fl.hourly_price) AS avg_hourly_price,
  abs(avg(fl.hourly_price - cpl.avg_hourly_price)) mad_hourly_price,
  max(fl.hourly_price) AS max_hourly_price,
  min(fl.hourly_price) AS min_hourly_price,
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
GROUP BY ALL
ORDER BY
  1,
  2,
  3,
  4
