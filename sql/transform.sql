  ---------------------------------------------------------------------
  --step1.staging
  ---------------------------------------------------------------------
  -- Drop the table if it already exists
DROP TABLE IF EXISTS
  `grand-water-473707-r8.osm_demo.staging_charging_station`;
  -- Create the new table with your logic
CREATE TABLE
  `grand-water-473707-r8.osm_demo.staging_charging_station` AS
WITH
  latest AS (
  SELECT
    gcp.place_id,
    MAX(gcp.fetched_at) AS max_fetched_at
  FROM
    `grand-water-473707-r8.osm_demo.google_charging_places` gcp
  GROUP BY
    ALL )
SELECT
  gcp.place_id,
  CAST(DATE_TRUNC(gcp.fetched_at, day) AS date) AS fetched_at,
  gcp.name,
  CASE
    WHEN LOWER(gcp.name) LIKE '%charging%station%' THEN SPLIT(gcp.name, ' ')[ OFFSET (0)]
    ELSE NULL
END
  AS brand,
  gcp.address,
  gcp.lat,
  gcp.lng,
  gcp.google_maps_url,
  gcp.search_keyword,
  gcp.search_radius_m,
  gcp.types,
  gcp.rating,
  gcp.user_ratings_total,
FROM
  `grand-water-473707-r8.osm_demo.google_charging_places` gcp
INNER JOIN
  latest l
ON
  l.place_id = gcp.place_id
  AND l.max_fetched_at = gcp.fetched_at ;
  ---------------------------------------------------------------------
  --step2.fact
  ---------------------------------------------------------------------
  -- Drop the table if it already exists
DROP TABLE IF EXISTS
  `grand-water-473707-r8.osm_demo.fact_charging_station`;
  -- Create the new table with your logic
CREATE TABLE
  `grand-water-473707-r8.osm_demo.fact_charging_station` AS
SELECT
  scs.place_id,
  scs.rating,
  scs.user_ratings_total
FROM
  `grand-water-473707-r8.osm_demo.staging_charging_station` scs ;
  ---------------------------------------------------------------------
  --step3.dim
  ---------------------------------------------------------------------
  -- Drop the table if it already exists
DROP TABLE IF EXISTS
  `grand-water-473707-r8.osm_demo.dim_charging_station`;
  -- Create the new table with your logic
CREATE TABLE
  `grand-water-473707-r8.osm_demo.dim_charging_station` AS
SELECT
  scs.place_id,
  scs.fetched_at,
  scs.name,
  scs.brand,
  scs.address,
  scs.lat,
  scs.lng,
  scs.google_maps_url,
  scs.search_keyword,
  scs.types
FROM
  `grand-water-473707-r8.osm_demo.staging_charging_station` scs
