----------------------------------------------------------------------------
--Basic select & cleanup
----------------------------------------------------------------------------

--RAW
SELECT * FROM `grand-water-473707-r8.raw.raw_parkbee_garages`;
delete from `grand-water-473707-r8.raw.raw_parkbee_garages` where 1 = 1;

SELECT * FROM `grand-water-473707-r8.raw.raw_google_charging_places` LIMIT 1000
SELECT date_trunc(scrape_datetime, day) as scrape_datetime, address.country, count(*) FROM `grand-water-473707-r8.raw.raw_parkbee_garages` group by all
  
  --STAGING
drop table `grand-water-473707-r8.staging.staging_parkbee_garages`;
delete from `grand-water-473707-r8.staging.staging_parkbee_garages` where 1 = 1;
select * from `grand-water-473707-r8.staging.staging_parkbee_garages`
select date_trunc(scrape_datetime_cet, day) as scrape_datetime_cet, country, count(*) from `grand-water-473707-r8.staging.staging_parkbee_garages` group by all
  
--DWH
select * from `grand-water-473707-r8.dwh.dim_parkbee_locations` order by 1;
select * from `grand-water-473707-r8.dwh.fact_parkbee_locations` limit 10;

----------------------------------------------------------------------------
--Count check
----------------------------------------------------------------------------

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

--Check park_from / park_to UTC / CET
select r.id, r.parking_from, r.parking_to, s.parking_from_cet, s.parking_to_cet
from `grand-water-473707-r8.raw.raw_parkbee_garages` r
inner join `grand-water-473707-r8.staging.staging_parkbee_garages` s
on s.location_id = r.id
where r.id = '731663e5-7559-4914-a824-ef53ecbee8d2'
