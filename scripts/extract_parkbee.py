import json
import sys
from datetime import datetime, timezone

# --------------------------------------------------------------
# Usage
# --------------------------------------------------------------
if len(sys.argv) != 2:
    print("Usage: python extract_parkbee.py garages.har")
    sys.exit(1)

har_file = sys.argv[1]

# --------------------------------------------------------------
# Extract parking window from HAR
# --------------------------------------------------------------
def parse_parking_window(har):
    """
    Parse searchParams or bookingParams to extract:
      parking_from, parking_to, parking_duration_hours
    Supports:
      - variables.searchParams
      - variables.bookingParams
      - nested variables.operations.variables
    """

    for entry in har.get("log", {}).get("entries", []):
        try:
            req = entry.get("request", {})
            text = req.get("postData", {}).get("text", "")

            if not text or not text.startswith("{"):
                continue

            obj = json.loads(text)
            variables = obj.get("variables", {})

            # Build list of candidate containers to scan
            candidates = [variables]

            # nested operations
            if "operations" in variables:
                for op in variables["operations"]:
                    if "variables" in op:
                        candidates.append(op["variables"])

            # scan each candidate
            for c in candidates:

                # Case 1 — searchParams
                if "searchParams" in c:
                    sp = c["searchParams"]
                    if "from" in sp and "to" in sp:
                        dt_from = sp["from"]
                        dt_to = sp["to"]

                # Case 2 — bookingParams
                elif "bookingParams" in c:
                    bp = c["bookingParams"]
                    if "from" in bp and "to" in bp:
                        dt_from = bp["from"]
                        dt_to = bp["to"]

                else:
                    continue

                # parse times
                f = datetime.fromisoformat(dt_from.replace("Z", "+00:00"))
                t = datetime.fromisoformat(dt_to.replace("Z", "+00:00"))
                duration_hours = round((t - f).total_seconds() / 3600, 2)

                return dt_from, dt_to, duration_hours

        except Exception:
            continue

    return None, None, None


# --------------------------------------------------------------
# Load HAR
# --------------------------------------------------------------
with open(har_file, "r") as f:
    har = json.load(f)

parking_from, parking_to, parking_duration_hours = parse_parking_window(har)

# --------------------------------------------------------------
# Extract garages
# --------------------------------------------------------------
garages = {}

for entry in har["log"]["entries"]:
    try:
        req = entry["request"]
        res = entry["response"]
        url = req["url"]

        if "/graphql" not in url:
            continue

        text = res.get("content", {}).get("text", "")
        if not text or not text.startswith("{"):
            continue

        obj = json.loads(text)

        if "data" not in obj:
            continue
        if "searchGarages" not in obj["data"]:
            continue

        for g in obj["data"]["searchGarages"]:
            garage_id = g.get("id")
            if not garage_id:
                continue

            # hourly price
            pricing = g.get("pricingAndAvailability", {}).get("pricing", {})
            total_cost = pricing.get("cost")

            if total_cost is not None and parking_duration_hours:
                hourly_price = round(float(total_cost) / parking_duration_hours, 4)
            else:
                hourly_price = None

            garages[garage_id] = {
                "id": garage_id,
                "name": g.get("name"),
                "latitude": g.get("latitude"),
                "longitude": g.get("longitude"),
                "address": g.get("address"),
                "pricingAndAvailability": g.get("pricingAndAvailability"),
                "scrape_datetime": datetime.now(timezone.utc).isoformat(),
                "parking_from": parking_from,
                "parking_to": parking_to,
                "parking_duration_hours": parking_duration_hours,
                "hourly_price": hourly_price,
            }

    except Exception:
        continue

print(f"📦 Extracted {len(garages)} garages")

# --------------------------------------------------------------
# Save output as NDJSON (required for BigQuery)
# --------------------------------------------------------------
output_filename = f"parkbee_garages_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

with open(output_filename, "w") as f:
    for g in garages.values():
        f.write(json.dumps(g) + "\n")

print(f"💾 Saved NDJSON → {output_filename}")
