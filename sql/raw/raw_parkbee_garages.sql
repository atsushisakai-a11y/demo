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
  operator STRUCT<
    name STRING
  >,
  scrape_datetime TIMESTAMP
);
