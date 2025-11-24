import json
import sys
from datetime import datetime, timezone

if len(sys.argv) != 2:
    print("Usage: python extract_parkbee.py garages.har")
    sys.exit(1)

har_file = sys.argv[1]

# ----------------------------------------------------------------------
# Helper: Extract parking window (from, to, duration)
# ----------------------------------------------------------------------
def parse_parking_window(har):
    """
    Finds parking from/to inside HAR.
    Supports:
      - variables.searchParams
      - variables.bookingParams
      - nested mutation/query blocks
    Returns (from_iso, to_iso, duration_hours)
    """

    for entry in har.get("log", {}).get("entries", []):
        try:
            req = entry.get("request", {})
            txt = req.get("postData", {}).get("text", "")

            if not txt or not txt.startswith("{"):
                continue

            body = json.loads(txt)

            # The field can be at different levels
            variables = body.get("variables", {})

            candidate_blocks = []

            # Case 1: direct variables
            candidate_blocks.append(variables)

            # Case 2: operations inside queries/mutations
            if "operations" in variables:
                for op in variables["operations"]:
                    if "variables" in op:
                        candidate_blocks.append(op["variables"])

            # Scan each candidate
            for block in candidate_blocks:

                # ---- searchParams ----
                if "searchParams" in block:
                    sp = block["searchParams"]
                    if "from" in sp and "to" in sp:
                        dt_from = sp["from"]
                        dt_to = sp["to"]
                    else:
                        continue

                # ---- bookingParams ----
                elif "bookingParams" in block:
                    bp = block["bookingParams"]
                    if "from" in bp and "to" in bp:
                        dt_from = bp["from"]
                        dt_to = bp["to"]
                    else:
                        continue

                else:
                    continue

                # Parse into datetime objects
                f = datetime.fromisoformat(dt_from.replace("Z", "+00:00"))
                t = datetime.fromisoformat(dt_to.replace("Z", "+00:00"))
                duration_hours = round((t - f).total_seconds() / 3600, 2)

                return dt_from, dt_to, duration_hours

        except Exception:
            continue

    return None, None, None


# ----------------------------------------------------------------------
# Load HAR
# ----------------------------------------------------------------------
with open(har_file, "r") as f:
    har = json.load(f)

# Get parking time range
parking_from, parking_to, parking_duration_hours = parse_parking_window(har)

# ----------------------------------------------------------------------
# Extract garages
# ----------------------------------------------------------------------
garages = {}

for entry in har["log"]["entries"]:
    try:
        req = entry["request"]
        res = entry["response"]
        url = req["url"]

        # Only GraphQL
        if "/graphql" not in url:
            continue

        text = res.get("content", {}).get("text", "")
        if not text or not text.startswith("{"):
            continue

        body = json.loads(text)

        if "data" not in body:
            continue
        if "searchGarages" not in body["data"]:
            continue

        for g in body["data"]["searchGarages"]:
            gid = g.get("id")
            if gid:

                # Calculate hourly price
                pricing = g.get("pricingAndAvailability", {}).get("pricing", {})
                total_cost = pricing.get("cost")
                if total_cost is not None and parking_duration_hours:
                    hourly_price = round(float(total_cost) / parking_duration_hours, 4)
                else:
                    hourly_price = None

                garages[gid] = {
                    "id": gid,
                    "name": g.get("name"),
                    "latitude": g.get("latitude"),
                    "longitude": g.get("longitude"),
                    "address": g.get("address"),
                    "pricingAndAvailability": g.get("pricingAndAvailability"),
                    "scrape_datetime": datetime.now(timezone.utc).isoformat(),

                    # newly added fields
                    "parking_from": parking_from,
                    "parking_to": parking_to,
                    "parking_duration_hours": parking_duration_hours,
                    "hourly_price": hourly_price,
                }

    except Exception:
        continue

print(f"📦 Extracted {len(garages)} garages")

# ----------------------------------------------------------------------
# Save JSON
# ----------------------------------------------------------------------
output_file = f"parkbee_garages_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

with open(output_file, "w") as f:
    json.dump(list(garages.values()), f, indent=2)

print(f"💾 Saved → {output_file}")
