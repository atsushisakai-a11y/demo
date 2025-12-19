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
    CASE
        WHEN pricingAndAvailability.availability.totalSpaces > 0 THEN 
            (pricingAndAvailability.availability.totalSpaces - pricingAndAvailability.availability.availableSpaces) / pricingAndAvailability.availability.totalSpaces
        ELSE NULL
    END AS utilization_rate,
    scrape_datetime AS scrape_datetime_cet,
    parking_from AS parking_from_cet,
    parking_to AS parking_to_cet,
    EXTRACT(HOUR FROM parking_from) AS parking_from_hour,
    EXTRACT(HOUR FROM parking_to) AS parking_to_hour,
    FORMAT_TIMESTAMP('%a', parking_from) AS parking_from_weekday,
    parking_duration_hours,
    SAFE_DIVIDE(pricingAndAvailability.pricing.cost, parking_duration_hours) AS hourly_price
FROM
    {{ source('raw', 'raw_parkbee_garages') }}
WHERE
    id IS NOT NULL
