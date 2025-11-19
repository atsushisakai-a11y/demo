CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.fact_parkbee_locations` AS
SELECT
  spg.scrape_datetime,
  spg.location_id,
  spg.price_cost,
  spg.available_spaces,
  spg.total_spaces,
  spg.total_spaces - spg.available_spaces as occupancy,
  case when spg.total_spaces > 0 then (spg.total_spaces - spg.available_spaces) / spg.total_spaces else null end as occupancy_rate
FROM
  `grand-water-473707-r8.staging.staging_parkbee_garages` spg
order by 1,2,3
