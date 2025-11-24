import json
import sys
from datetime import datetime, timezone

# -----------------------------------------
# Helper: safely get nested fields
# -----------------------------------------
def safe_get(obj, *keys):
    for k in keys:
        if obj is None:
            return None
        obj = obj.get(k)
    return obj


# -----------------------------------------
# Helper: extract duration hours
# -----------------------------------------
def compute_duration_hours(start_iso, end_iso):
    try:
        start = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
        end = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
        diff = end - start
        hours = diff.total_seconds() / 3600
        return round(hours, 2)
    except Exception:
        return None


# -----------------------------------------
# Parse arguments
# -----------------------------------------
if len(sys.argv) != 2:
    print("Usage: python extract_parkbee.py garages.har")
    sys.exit(1)

har_file = sys.argv[1]

# -----------------------------------------
# Load HAR file
# -----------------------------------------
with open(har_file, "r") as f:
    har = json.load(f)

garages = {}

# -----------------------------------------
# Scan HAR entries
# -----------------------------------------
for entry in har["log"]["entries"]:
    try:
        req = entry.get("request", {})
        res = entry.get("response", {})
        url = req.get("url", "")

        # We only want GraphQL queries returning garage data
        if "/graphql" not in url:
            continue

        text = res.get("content", {}).get("text")
        if not text or not text.startswith("{"):
            continue

        body = json.loads(text)
        data = body.get("data", {})
        garages_list = data.get("searchGarages")
        if garages_list is None:
            continue

        # Extract parking session info from request payload
        parking_from = None
        parking_to = None
        parking_duration_hours = None

        post_data = safe_get(req, "postData", "text")
        if post_data:
            try:
                payload = json.loads(post_data)
                variables = payload.get("variables", {})

                parking_from = variables.get("from")
                parking_to = variables.get("to")

                if parking_from and parking_to:
                    parking_duration_hours = compute_duration_hours(parking_from, parking_to)

            except Exception:
                pass  # silently ignore malformed payload

        # Extract each garage
        for g in garages_list:
            gid = g.get("id")
            if not gid:
                continue

            # Extract cost & compute hourly price
            pricing = safe_get(g, "pricingAndAvailability", "pricing", "cost")
            if pricing is None:
                hourly_price = None
            else:
                try:
                    hourly_price = round(pricing / parking_duration_hours, 4) if parking_duration_hours else None
                except Exception:
                    hourly_price = None

            garages[gid] = {
                "id": gid,
                "name": g.get("name"),
                "latitude": g.get("latitude"),
                "longitude": g.get("longitude"),
                "address": g.get("address"),
                "pricingAndAvailability": g.get("pricingAndAvailability"),
                "scrape_datetime": datetime.now(timezone.utc).isoformat(),

                # NEW FIELDS
                "parking_from": parking_from,
                "parking_to": parking_to,
                "parking_duration_hours": parking_duration_hours,
                "hourly_price": hourly_price,
            }

    except Exception:
        continue  # ignore entry-level errors


# -----------------------------------------
# Save output JSON
# -----------------------------------------
output_list = list(garages.values())
out_file = f"parkbee_garages_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

with open(out_file, "w") as f:
    json.dump(output_list, f, indent=2)

print(f"📦 Extracted {len(output_list)} garages")
print(f"💾 Saved → {out_file}")
