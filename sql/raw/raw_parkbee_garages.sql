-- DROP OLD TABLE (if exists)
DROP TABLE IF EXISTS `grand-water-473707-r8.raw.raw_parkbee_garages`;

-- CREATE NEW RAW TABLE
CREATE TABLE `grand-water-473707-r8.raw.raw_parkbee_garages` (
  id STRING,
  name STRING,
  latitude FLOAT64,
  longitude FLOAT64,

  address STRUCT<
    city STRING,
    country STRING,
    street STRING,
    number STRING,
    postCode STRING
  >,

  pricingAndAvailability STRUCT<
    pricing STRUCT<
      cost FLOAT64,
      currency STRING
    >,
    availability STRUCT<
      availableSpaces INT64,
      totalSpaces INT64
    >
  >,

  scrape_datetime TIMESTAMP,

  -- NEW FIELDS
  parking_from TIMESTAMP,
  parking_to TIMESTAMP,
  parking_duration_hours FLOAT64,
  hourly_price FLOAT64
)
OPTIONS(
  description="ParkBee raw garages data including pricing and parking duration metadata"
);
