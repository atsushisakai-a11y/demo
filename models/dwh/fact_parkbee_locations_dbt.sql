{{ config(
    materialized = "table"
) }}

WITH spg AS (
    SELECT * 
    FROM {{ ref('staging_parkbee_garages_dbt') }}
)

SELECT
    spg.location_id,
    spg.scrape_datetime_cet,
    spg.parking_from_cet,
    spg.parking_to_cet,

    -- Hour extracted from parking times
    EXTRACT(HOUR FROM spg.parking_from_cet) AS parking_from_hour,
    EXTRACT(HOUR FROM spg.parking_to_cet) AS parking_to_hour,

    -- Weekday abbreviation (Mon/Tue/Wed…)
    FORMAT_TIMESTAMP('%a', spg.parking_from_cet) AS parking_from_weekday,

    spg.price_cost,
    spg.parking_duration_hours,

    -- Avoid division error
    SAFE_DIVIDE(spg.price_cost, spg.parking_duration_hours) AS hourly_price,

    spg.available_spaces,
    spg.total_spaces,

    -- Occupancy rate = (total - available) / total
    CASE
        WHEN spg.total_spaces > 0 THEN 
            (spg.total_spaces - spg.available_spaces) / spg.total_spaces
        ELSE NULL
    END AS occupancy_rate

FROM spg
