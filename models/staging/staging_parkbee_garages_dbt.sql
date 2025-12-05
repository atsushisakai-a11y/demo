{{ config(
    materialized = "table"
) }}

-- Staging model for ParkBee garages

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
    scrape_datetime AS scrape_datetime_cet,
    parking_from AS parking_from_cet,
    parking_to AS parking_to_cet,
    parking_duration_hours,
    hourly_price
FROM
    {{ source('raw', 'raw_parkbee_garages') }}
WHERE
    id IS NOT NULL
