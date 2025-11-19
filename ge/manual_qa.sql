
SELECT
'staging_parkbee_garages' as table_name,
  DATE_TRUNC(scrape_datetime, day) AS scrape_date,
  COUNT(*)
FROM
  `grand-water-473707-r8.staging.staging_parkbee_garages`
GROUP BY
  ALL

union all

SELECT
'staging_google_parkbee_locations' as table_name,
  DATE_TRUNC(fetched_at, day) AS scrape_date,
  COUNT(*)
FROM
  `grand-water-473707-r8.staging.staging_google_parkbee_locations`
GROUP BY
  ALL

union all

SELECT
'staging_parkbee_locations_combined' as table_name,
  DATE_TRUNC(scrape_datetime_parkbee, day) AS scrape_date,
  COUNT(*)
FROM
  `grand-water-473707-r8.staging.staging_parkbee_locations_combined`
GROUP BY
  ALL

  union all

SELECT
'fact_parkbee_locations' as table_name,
  DATE_TRUNC(scrape_datetime_parkbee, day) AS scrape_date,
  COUNT(*)
FROM
  `grand-water-473707-r8.dwh.fact_parkbee_locations`
GROUP BY

--location_id duplication check
select 
dpl.location_id,
count(*) as counts
from `grand-water-473707-r8.dwh.dim_parkbee_locations` dpl
group by all
having count(*) > 1
order by 2 desc  
  ALL
ORDER BY
  1
