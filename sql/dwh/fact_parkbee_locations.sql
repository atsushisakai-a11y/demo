CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.fact_parkbee_locations` AS
SELECT
  spg.location_id,
  ST_GEOGPOINT(spg.longitude, spg.latitude) AS geom,
  spg.scrape_datetime_cet,
  spg.parking_from_cet,
  spg.parking_to_cet,
  spg.price_cost,
  spg.parking_duration_hours,
  SAFE_DIVIDE(spg.price_cost, spg.parking_duration_hours) AS hourly_price,
  spg.available_spaces,
  spg.total_spaces,
  CASE
    WHEN spg.total_spaces > 0 THEN (spg.total_spaces - spg.available_spaces) / spg.total_spaces
    ELSE NULL
END
  AS occupancy_rate
FROM
  `grand-water-473707-r8.staging.staging_parkbee_garages` spg
  ;
