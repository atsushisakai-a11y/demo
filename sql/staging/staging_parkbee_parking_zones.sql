CREATE OR REPLACE TABLE `grand-water-473707-r8.staging.staging_parkbee_parking_zones` AS
WITH pb AS (
  SELECT
    location_id,
    name,
    city,
    country,
    latitude,
    longitude,
    price_cost,
    price_currency,
    available_spaces,
    total_spaces,
    scrape_datetime_cet,
    ST_GEOGPOINT(longitude, latitude) AS pb_point
  FROM `grand-water-473707-r8.staging.staging_parkbee_garages`
),

zones AS (
  SELECT
    zone_id,
    hourly_rate,
    periode,
    days,
    geom,

    -- FIX: derive the centroid from the polygon
    ST_Y(ST_CENTROID(geom)) AS zone_center_lat,
    ST_X(ST_CENTROID(geom)) AS zone_center_lng

  FROM `grand-water-473707-r8.staging.staging_parking_fee_amsterdam`
),

joined AS (
  SELECT
    pb.location_id,
    pb.name AS parkbee_name,
    pb.city,
    pb.country,
    pb.latitude AS parkbee_lat,
    pb.longitude AS parkbee_lng,
    pb.price_cost AS parkbee_price_per_hour,
    pb.available_spaces,
    pb.total_spaces,
    pb.scrape_datetime_cet,

    z.zone_id,
    z.hourly_rate AS city_price_per_hour,
    z.periode AS valid_period,
    z.days AS valid_days,

    ST_DISTANCE(
      pb.pb_point,
      ST_GEOGPOINT(z.zone_center_lng, z.zone_center_lat)
    ) AS distance_to_zone_center,

    CASE
      WHEN z.hourly_rate IS NULL THEN NULL
      WHEN pb.price_cost IS NULL THEN NULL
      WHEN pb.price_cost < z.hourly_rate * 0.8 THEN 'CHEAP'
      WHEN pb.price_cost <= z.hourly_rate * 1.2 THEN 'FAIR'
      ELSE 'EXPENSIVE'
    END AS price_position

  FROM pb
  LEFT JOIN zones z
    ON ST_WITHIN(pb.pb_point, z.geom)
)

SELECT * FROM joined;
