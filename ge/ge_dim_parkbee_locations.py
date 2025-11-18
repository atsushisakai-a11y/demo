import great_expectations as ge
from google.cloud import bigquery

# Connect to BigQuery
client = bigquery.Client()

query = """
SELECT * FROM `grand-water-473707-r8.dwh.dim_parkbee_locations`
"""
df = client.query(query).to_dataframe()

# Create GE object
ge_df = ge.from_pandas(df)

# Define expectations
ge_df.expect_column_values_to_be_unique("location_id")
ge_df.expect_column_values_to_be_unique("place_id")
ge_df.expect_column_values_to_be_unique("google_maps_url")

# Optionally enforce no duplicates across multiple columns
ge_df.expect_select_column_values_to_be_unique(
    ["parkbee_lat", "parkbee_lng"],
    result_format="COMPLETE"
)

# Run the validations
results = ge_df.validate()

print(results)
assert results["success"], "❌ QA Failed: Duplicate values detected!"
print("🎉 All QA checks passed!")
