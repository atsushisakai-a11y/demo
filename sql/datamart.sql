DROP TABLE IF EXISTS
  `grand-water-473707-r8.osm_demo.datamart_charging_station`;
  -- Create the new table with your logic
CREATE TABLE
  `grand-water-473707-r8.osm_demo.datamart_charging_station` AS
SELECT
  dcs.search_keyword,
  COUNT(DISTINCT dcs.place_id) AS places,
  SUM(fcs.rating) AS rating,
  SUM(fcs.user_ratings_total) AS user_ratings_total,
  ROUND(SUM(fcs.rating) / SUM(fcs.user_ratings_total),1) AS average_rating
FROM
  `grand-water-473707-r8.osm_demo.dim_charging_station` dcs
INNER JOIN
  `grand-water-473707-r8.osm_demo.fact_charging_station` fcs
ON
  fcs.place_id = dcs.place_id
GROUP BY
  ALL
ORDER BY
  1
