google-maps-bq-pipeline/
│
├── dags/
│   ├── google_maps_bigquery_etl_dag.py        # Main Airflow DAG
│   ├── __init__.py
│
├── etl/
│   ├── google_maps_to_bq.py                   # Your Python ETL script
│   ├── __init__.py
│
├── sql/
│   ├── transform_staging.sql                  # SQL to transform staging table
│   ├── dim_charging_station.sql               # Example: create dimension table
│   ├── fact_usage_stats.sql                   # Example: create fact table
│
├── config/
│   ├── config.yaml                            # Project, dataset, API key, etc.
│   └── credentials.json                       # Optional local service account (gitignore)
│
├── utils/
│   ├── bq_helpers.py                          # Optional helper (e.g. load to BigQuery)
│   ├── gmaps_helpers.py                       # Helper functions for Google Maps API
│
├── tests/
│   ├── test_google_maps_to_bq.py              # Unit tests for ETL logic
│
├── .env.example                               # Example of environment variables
├── requirements.txt                           # Python dependencies
├── README.md                                  # Setup instructions
├── LICENSE
└── .gitignore
