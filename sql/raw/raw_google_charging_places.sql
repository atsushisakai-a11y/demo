CREATE OR REPLACE TABLE `grand-water-473707-r8.raw.raw_google_charging_places` (
  place_id             STRING,
  name                 STRING,
  address              STRING,
  lat                  FLOAT64,
  lng                  FLOAT64,
  types                STRING,
  rating               FLOAT64,
  user_ratings_total   INT64,
  google_maps_url      STRING,

  search_keyword       STRING,
  search_radius_m      INT64,

  -- EV-specific fields
  connector_type       STRING,
  power_kw             FLOAT64,
  available_count      FLOAT64,
  total_count          FLOAT64,
  charging_info_raw    STRING,

  -- Parking-specific fields
  is_parking           BOOL,
  parking_address      STRING,
  parking_summary      STRING,
  parking_types_raw    STRING,

  fetched_at           TIMESTAMP
)
OPTIONS (
  description = "Raw Google Places results for EV charging stations and parking facilities, including EV details, parking metadata, country, and city."
);
