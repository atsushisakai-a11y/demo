----------------------------------------------------------------------------
--counts per hour
----------------------------------------------------------------------------
WITH raw AS (
  SELECT
    address.country AS country,
    CAST(DATE_TRUNC(scrape_datetime, DAY) AS DATE) AS scrape_date,
    EXTRACT(HOUR FROM parking_from) AS parking_from_hour,
    COUNT(*) AS raw_count
  FROM `grand-water-473707-r8.raw.raw_parkbee_garages`
  GROUP BY ALL
),

dwh AS (
  SELECT
    dpl.country AS country,
    CAST(DATE_TRUNC(fpl.parking_from_cet, DAY) AS DATE) AS scrape_date,
    fpl.parking_from_hour,
    COUNT(*) AS dwh_count
  FROM `grand-water-473707-r8.dwh.fact_parkbee_locations_dbt` fpl
  INNER JOIN `grand-water-473707-r8.dwh.dim_parkbee_locations_dbt` dpl
    ON dpl.location_id = fpl.location_id
  GROUP BY ALL
),

datamart AS (
  SELECT
    country,
    parking_date AS scrape_date,
    parking_from_hour,
    COUNT(DISTINCT location_id) AS datamart_count
  FROM `grand-water-473707-r8.datamart.datamart_price_trend`
  GROUP BY ALL
)

SELECT
  COALESCE(raw.country, dwh.country, datamart.country) AS country,
  COALESCE(raw.scrape_date, dwh.scrape_date, datamart.scrape_date) AS scrape_date,
  COALESCE(raw.parking_from_hour, dwh.parking_from_hour, datamart.parking_from_hour) AS parking_from_hour,
  raw.raw_count,
  dwh.dwh_count,
  datamart.datamart_count
FROM raw
FULL OUTER JOIN dwh
  ON raw.country = dwh.country
  AND raw.scrape_date = dwh.scrape_date
  AND raw.parking_from_hour = dwh.parking_from_hour
FULL OUTER JOIN datamart
  ON COALESCE(raw.country, dwh.country) = datamart.country
  AND COALESCE(raw.scrape_date, dwh.scrape_date) = datamart.scrape_date
  AND COALESCE(raw.parking_from_hour, dwh.parking_from_hour) = datamart.parking_from_hour
ORDER BY
  country,
  scrape_date,
  parking_from_hour;



----------------------------------------------------------------------------
--Basic select & cleanup
----------------------------------------------------------------------------

--RAW
SELECT * FROM `grand-water-473707-r8.raw.raw_parkbee_garages`;
delete from `grand-water-473707-r8.raw.raw_parkbee_garages` where 1 = 1;

SELECT * FROM `grand-water-473707-r8.raw.raw_google_charging_places` LIMIT 1000
SELECT address.country, date_trunc(scrape_datetime, day) as scrape_datetime, count(*) FROM `grand-water-473707-r8.raw.raw_parkbee_garages` group by all order by 1,2
SELECT
address.country,
cast(date_trunc(scrape_datetime, day) as date) as scrape_datetime,
EXTRACT(HOUR FROM parking_from) AS parking_from_hour,
count(*)
FROM `grand-water-473707-r8.raw.raw_parkbee_garages` group by all order by 1,2,3,4

  SELECT
address.country,
cast(date_trunc(scrape_datetime, day) as date) as scrape_datetime,
EXTRACT(HOUR FROM parking_from) AS parking_from_hour,
count(*)
FROM `grand-water-473707-r8.raw.raw_parkbee_garages` group by all order by 1,2,3,4

  
  --STAGING
drop table `grand-water-473707-r8.staging.staging_parkbee_garages`;
delete from `grand-water-473707-r8.staging.staging_parkbee_garages` where 1 = 1;
select * from `grand-water-473707-r8.staging.staging_parkbee_garages`
select date_trunc(scrape_datetime_cet, day) as scrape_datetime_cet, country, count(*) from `grand-water-473707-r8.staging.staging_parkbee_garages` group by all;
select * from `grand-water-473707-r8.staging.staging_google_parkbee_locations` limit 100;
select * from `grand-water-473707-r8.staging.staging_google_parking_places` limit 100;
  
--DWH
select * from `grand-water-473707-r8.dwh.dim_parkbee_locations` order by 1;
select * from `grand-water-473707-r8.dwh.fact_parkbee_locations` limit 10;
select * from `grand-water-473707-r8.dwh.dim_google_places`;
select country, first_seen_date, count(*) from `grand-water-473707-r8.dwh.dim_parkbee_locations` group by all order by 1,2
  
--DATAMART
select * from `grand-water-473707-r8.datamart.datamart_google_places_nearest_parkbee`
select country, scrape_date,count(distinct location_id) as location_ids, count(*) as locations
from `grand-water-473707-r8.datamart.datamart_price_comparison` group by all order by 1,2 limit 100;
select location_id,count(*) from `grand-water-473707-r8.datamart.datamart_price_comparison` group by all having count(*) > 1

select country, scrape_date,count(distinct location_id) as location_ids, count(*) as locations
from `grand-water-473707-r8.datamart.datamart_price_comparison_dbt` group by all order by 1,2 limit 100;

SELECT
  country,
  parking_date,
  parking_from_hour,
  count(distinct location_id) locations
FROM
  `grand-water-473707-r8.datamart.datamart_price_trend`
  group by all
  order by 1,2,3
LIMIT
  1000

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
