{{ config(
    materialized = "table"
) }}

WITH lvl1 AS (

    SELECT
        zone_id,
        SAFE.ST_GEOGFROMGEOJSON(location_json) AS geom,   -- polygon geometry
        description.__invalid_keys__[OFFSET(0)].value AS description,
        t AS tariff_struct
    FROM {{ source('raw', 'raw_parking_fee_amsterdam') }}
    CROSS JOIN UNNEST(tarieven) AS t

),

lvl2 AS (

    SELECT
        zone_id,
        geom,                       -- polygon
        description,

        -- Extract hourly price
        t1.key AS hourly_rate_str,

        -- Extract periode + days
        t1.value.__invalid_keys__[OFFSET(0)].key AS periode,
        t1.value.__invalid_keys__[OFFSET(0)].value AS days

    FROM lvl1
    CROSS JOIN UNNEST(tariff_struct.__invalid_keys__) AS t1

)

SELECT
    zone_id,
    description,
    geom,   -- original polygon

    SAFE_CAST(REPLACE(hourly_rate_str, ',', '.') AS FLOAT64) AS hourly_rate,
    periode,
    days,

    -- centroid for mapping / reference
    ST_Y(ST_CENTROID(geom)) AS zone_lat,
    ST_X(ST_CENTROID(geom)) AS zone_lng

FROM lvl2
