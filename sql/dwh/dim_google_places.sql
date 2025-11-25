CREATE OR REPLACE TABLE `grand-water-473707-r8.dwh.dim_google_places` AS
SELECT
  sg.place_id,
  sg.name,
  sg.address,
  sg.lat,
  sg.lng,
  ST_GEOGPOINT(sg.lng, sg.lat) AS geom,
  min(sg.fetched_at) as first_seen_date,
  max(sg.fetched_at) as last_seen_date
FROM
  `grand-water-473707-r8.staging.staging_google_parkbee_locations` sg
group by all
order by 1,2
