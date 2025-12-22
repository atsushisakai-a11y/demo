{{ config(
    materialized = "table",
    tags = ["datamart"]
) }}

WITH latest AS (
    SELECT
        fl.location_id,
        MAX(fl.scrape_datetime_cet) AS max_scrape_datetime_cet
    FROM {{ ref('fact_location') }} fl
    GROUP BY fl.location_id
)

SELECT
    CAST(DATE_TRUNC(fl.scrape_datetime_cet, DAY) AS DATE) AS scrape_date,
    fl.parking_from_cet,
    fl.parking_to_cet,
    dl.name,
    dl.location_id,
    dl.city,
    dl.country,
    dl.latitude,
    dl.longitude,
    fl.utilization_rate,
    fl.price_cost,
    fl.hourly_price,
    fl.available_spaces,
    fl.total_spaces,
    z.zone_id,
    z.hourly_rate AS public_hourly_price,
    (fl.hourly_price - z.hourly_rate) AS price_gap,

    CASE
        WHEN fl.hourly_price > z.hourly_rate THEN 'Public Cheaper'
        WHEN fl.hourly_price < z.hourly_rate THEN 'ParkBee Cheaper'
        ELSE 'No public parking data'
    END AS price_position

FROM {{ ref('fact_location') }} fl

INNER JOIN latest l
    ON  l.location_id = fl.location_id
    AND l.max_scrape_datetime_cet = fl.scrape_datetime_cet

INNER JOIN {{ ref('dim_location') }} dl
    ON dl.location_id = fl.location_id

LEFT JOIN {{ ref('fact_parking_fee_amsterdam') }} z
    ON ST_WITHIN(dl.geom, z.geom)

WHERE
    fl.utilization_rate >= 0
    AND fl.utilization_rate <= 1
    AND fl.hourly_price <= 20
    AND dl.platform = 'parkbee'
ORDER BY
    scrape_date,
    fl.parking_from_cet
