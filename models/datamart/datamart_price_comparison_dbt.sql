{{ config(
    materialized = "table",
    tags = ["datamart"]
) }}

WITH latest AS (
    SELECT
        fpl.location_id,
        MAX(fpl.scrape_datetime_cet) AS max_scrape_datetime_cet
    FROM {{ ref('fact_parkbee_locations_dbt') }} fpl
    GROUP BY fpl.location_id
)

SELECT
    CAST(DATE_TRUNC(fpl.scrape_datetime_cet, DAY) AS DATE) AS scrape_date,
    fpl.parking_from_cet,
    fpl.parking_to_cet,
    dpl.name,
    dpl.location_id,
    dpl.city,
    dpl.country,
    dpl.latitude,
    dpl.longitude,
    fpl.occupancy_rate,
    fpl.price_cost,
    fpl.hourly_price,
    fpl.available_spaces,
    fpl.total_spaces,
    z.zone_id,
    z.hourly_rate AS public_hourly_price,
    (fpl.hourly_price - z.hourly_rate) AS price_gap,

    CASE
        WHEN fpl.hourly_price > z.hourly_rate THEN 'Public Cheaper'
        WHEN fpl.hourly_price < z.hourly_rate THEN 'ParkBee Cheaper'
        ELSE 'No public parking data'
    END AS price_position

FROM {{ ref('fact_parkbee_locations_dbt') }} fpl

INNER JOIN latest l
    ON  l.location_id = fpl.location_id
    AND l.max_scrape_datetime_cet = fpl.scrape_datetime_cet

INNER JOIN {{ ref('dim_parkbee_locations_dbt') }} dpl
    ON dpl.location_id = fpl.location_id

LEFT JOIN {{ ref('fact_parking_fee_amsterdam_dbt') }} z
    ON ST_WITHIN(dpl.geom, z.geom)

WHERE
    fpl.occupancy_rate >= 0
    AND fpl.occupancy_rate <= 1
    AND fpl.hourly_price <= 20

ORDER BY
    scrape_date,
    fpl.parking_from_cet
