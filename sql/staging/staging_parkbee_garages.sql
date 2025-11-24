CREATE OR REPLACE TABLE `grand-water-473707-r8.staging.staging_parkbee_garages` AS
SELECT
  id AS location_id,
  name,
  latitude,
  longitude,
  address.city AS city,
  address.country AS country,
  pricingAndAvailability.pricing.cost AS price_cost,
  pricingAndAvailability.pricing.currency AS price_currency,
  pricingAndAvailability.availability.availableSpaces AS available_spaces,
  pricingAndAvailability.availability.totalSpaces AS total_spaces,

  -- Convert timestamps to CET
  DATETIME(scrape_datetime, "Europe/Amsterdam") AS scrape_datetime_cet,
  DATETIME(parking_from,    "Europe/Amsterdam") AS parking_from_cet,
  DATETIME(parking_to,      "Europe/Amsterdam") AS parking_to_cet,

  parking_duration_hours,
  hourly_price

FROM `grand-water-473707-r8.raw.raw_parkbee_garages`
WHERE id IS NOT NULL;
