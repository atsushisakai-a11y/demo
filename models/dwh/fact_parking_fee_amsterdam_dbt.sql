{{ config(
    materialized = "table",
    tags = ["dwh"]
) }}

SELECT
    spfa.zone_id,
    spfa.days,
    spfa.description,
    spfa.hourly_rate,
    spfa.periode,
    spfa.zone_lat,
    spfa.zone_lng,
    spfa.geom
FROM {{ ref('staging_parking_fee_amsterdam_dbt') }} AS spfa
ORDER BY
    zone_id,
    days;
