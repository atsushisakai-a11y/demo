CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.fact_parkbee_locations` AS
SELECT
  splc.scrape_datetime_parkbee,
  splc.scrape_datetime_google,
  splc.location_id,
  splc.price_cost,
  splc.available_spaces,
  splc.total_spaces,
  splc.avg_rating,
  splc.total_review
FROM
  `grand-water-473707-r8.staging.staging_parkbee_locations_combined` splc
order by 1,2,3
