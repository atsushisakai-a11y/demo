{{ config(
    materialized = "table",
    tags = ["dwh"]
) }}

WITH latest AS (
    SELECT
        spg.location_id,
        MAX(spg.scrape_datetime_cet) AS last_seen_datetime,
        MIN(spg.scrape_datetime_cet) AS first_seen_datetime
    FROM {{ ref('staging_parkbee_garages_dbt') }} spg
    GROUP BY spg.location_id
),

joined AS (
    SELECT
        spg.location_id,
        spg.country,
        spg.city,
        spg.name,
        spg.latitude,
        spg.longitude,
        ST_GEOGPOINT(spg.longitude, spg.latitude) AS geom,
        DATE_TRUNC(l.first_seen_datetime, DAY) AS first_seen_date,
        DATE_TRUNC(l.last_seen_datetime, DAY) AS last_seen_date
    FROM {{ ref('staging_parkbee_garages_dbt') }} spg
    INNER JOIN latest l
        ON l.location_id = spg.location_id
       AND l.last_seen_datetime = spg.scrape_datetime_cet
)

SELECT *
FROM joined
ORDER BY
    location_id,
    country,
    city
