CREATE OR REPLACE TABLE `grand-water-473707-r8.staging.staging_parkbee_garages` AS
SELECT
  id,
  name,
  latitude,
  longitude,
  address.city AS city,
  address.country AS country,
  pricingAndAvailability.pricing.cost AS price_cost,
  pricingAndAvailability.pricing.currency AS price_currency,
  pricingAndAvailability.availability.availableSpaces AS available_spaces,
  pricingAndAvailability.availability.totalSpaces AS total_spaces,
  scrape_datetime
FROM
  `grand-water-473707-r8.raw.raw_parkbee_garages`
WHERE id is not null  
  ;
