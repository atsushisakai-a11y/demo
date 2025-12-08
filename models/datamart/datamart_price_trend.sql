{{ config(
    materialized = "table",
    tags = ["datamart"]
) }}
SELECT
  dpl.country,
  dpl.city,
  dpl.name,
  dpl.location_id,
  cast(fpl.parking_from_cet as date) as parking_date,
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
where cast(fpl.parking_from_cet as date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
order by 1,2,3,4
