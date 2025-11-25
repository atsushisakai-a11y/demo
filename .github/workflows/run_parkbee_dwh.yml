import json
import sys
import re
from datetime import datetime
import pytz

# -----------------------------------------
# Helper: Convert ISO8601 (with CET offset) → UTC
# -----------------------------------------
def to_utc(dt_str):
    """
    Convert ISO8601 datetime with offset to UTC Z-format.
    Example:
        2025-11-21T22:30:00+01:00 → 2025-11-21T21:30:00Z
    """
    if dt_str is None:
        return None

    # Normalize Z notation
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")

    dt = datetime.fromisoformat(dt_str)  # Respect embedded offset
    dt_utc = dt.astimezone(pytz.UTC)
    return dt_utc.isoformat().replace("+00:00", "Z")


# -----------------------------------------
# Read CLI argument
# -----------------------------------------
if len(sys.argv) != 2:
    print("Usage: python extract_parkbee.py garages.har")
    sys.exit(1)

har_file = sys.argv[1]

with open(har_file, "r") as f:
    har = json.load(f)

garages = {}
parking_from = None
parking_to = None
scrape_datetime = None

# -----------------------------------------
# Step 1: Extract parking_from + parking_to from postData.variables.pricing
# -----------------------------------------
for entry in har["log"]["entries"]:
    req = entry.get("request", {})
    post = req.get("postData", {}).get("text", "")

    if not post or not post.startswith("{"):
        continue

    try:
        body = json.loads(post)
    except:
        continue

    variables = body.get("variables", {})

    # Look inside pricing block
    if "pricing" in variables:
        pf = variables["pricing"].get("startDateTime")
        pt = variables["pricing"].get("endDateTime")

        if pf and pt:
            parking_from = pf
            parking_to = pt

    # If already found both, stop searching
    if parking_from and parking_to:
        break

# -----------------------------------------
# Step 2: Find scrape_datetime from response.headers["date"]
# -----------------------------------------
for entry in har["log"]["entries"]:
    resp = entry.get("response", {})
    headers = resp.get("headers", [])

    for h in headers:
        if h.get("name", "").lower() == "date":
            # Example: "Fri, 21 Nov 2025 14:28:40 GMT"
            try:
                dt = datetime.strptime(h["value"], "%a, %d %b %Y %H:%M:%S GMT")
                dt = pytz.UTC.localize(dt)
                scrape_datetime = dt.isoformat().replace("+00:00", "Z")
            except:
                continue

    if scrape_datetime:
        break

# -----------------------------------------
# Fallback datetime if none found
# -----------------------------------------
if not scrape_datetime:
    scrape_datetime = datetime.utcnow().replace(tzinfo=pytz.UTC).isoformat().replace("+00:00", "Z")

# -----------------------------------------
# Convert extracted CET → UTC for BigQuery
# -----------------------------------------
parking_from_utc = to_utc(parking_from)
parking_to_utc = to_utc(parking_to)

# Calculate hours
parking_duration_hours = None
hourly_price = None

if parking_from_utc and parking_to_utc:
    dt_from = datetime.fromisoformat(parking_from_utc.replace("Z", "+00:00"))
    dt_to = datetime.fromisoformat(parking_to_utc.replace("Z", "+00:00"))
    duration = (dt_to - dt_from).total_seconds() / 3600
    parking_duration_hours = round(duration, 2) if duration > 0 else None


# -----------------------------------------
# Step 3: Extract garage results
# -----------------------------------------
for entry in har["log"]["entries"]:
    req = entry.get("request", {})
    res = entry.get("response", {})
    url = req.get("url", "")

    if "/graphql" not in url:
        continue

    text = res.get("content", {}).get("text", "")
    if not text or not text.startswith("{"):
        continue

    try:
        body = json.loads(text)
    except:
        continue

    data = body.get("data", {})
    if "searchGarages" not in data:
        continue

    for g in data["searchGarages"]:
        gid = g.get("id")
        if not gid:
            continue

        item = {
            "id": gid,
            "name": g.get("name"),
            "latitude": g.get("latitude"),
            "longitude": g.get("longitude"),
            "address": g.get("address"),
            "pricingAndAvailability": g.get("pricingAndAvailability"),

            # UTC timestamps for BigQuery ingestion
            "scrape_datetime": scrape_datetime,
            "parking_from": parking_from_utc,
            "parking_to": parking_to_utc,
            "parking_duration_hours": parking_duration_hours,
            "hourly_price": None
        }

        # Compute hourly price safely
        try:
            cost = g["pricingAndAvailability"]["pricing"]["cost"]
            if parking_duration_hours and parking_duration_hours > 0:
                item["hourly_price"] = round(cost / parking_duration_hours, 2)
        except:
            pass

        garages[gid] = item


print(f"📦 Extracted {len(garages)} garages")

# -----------------------------------------
# Output file
# -----------------------------------------
output_name = f"parkbee_garages_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"

with open(output_name, "w") as f:
    json.dump(list(garages.values()), f, indent=2)

print(f"💾 Saved → {output_name}")
