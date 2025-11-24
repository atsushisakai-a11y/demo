import json
import sys
from datetime import datetime

if len(sys.argv) != 2:
    print("Usage: python extract_parkbee.py garages.har")
    sys.exit(1)

har_file = sys.argv[1]

with open(har_file, "r") as f:
    har = json.load(f)

garages = {}
parking_from = None
parking_to = None

# 1. Extract date range from GraphQL request payload
for entry in har["log"]["entries"]:
    req = entry.get("request", {})
    if "/graphql" not in req.get("url", ""):
        continue
    try:
        payload = req.get("postData", {}).get("text", "")
        if not payload:
            continue
        body = json.loads(payload)

        # Look for structure containing parking times
        vars = body.get("variables", {})
        if "dateFrom" in vars and "dateTo" in vars:
            parking_from = vars["dateFrom"]
            parking_to = vars["dateTo"]
    except Exception:
        continue

# Convert timestamps
pf = datetime.fromisoformat(parking_from.replace("Z", "+00:00")) if parking_from else None
pt = datetime.fromisoformat(parking_to.replace("Z", "+00:00")) if parking_to else None

parking_duration_hours = None
if pf and pt:
    parking_duration_hours = (pt - pf).total_seconds() / 3600.0

# 2. Extract garages
for entry in har["log"]["entries"]:
    try:
        req = entry["request"]
        res = entry["response"]
        url = req["url"]

        if "/graphql" not in url:
            continue

        text = res["content"].get("text", "")
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
                garages[gid] = g

    except Exception:
        continue

print(f"📦 Extracted {len(garages)} garages")

# 3. Insert new parking fields + hourly price
output = []
for g in garages.values():
    price = g.get("pricingAndAvailability", {}).get("pricing", {}).get("cost")
    hourly_price = None
    if parking_duration_hours and parking_duration_hours > 0 and price:
        hourly_price = price / parking_duration_hours

    g["parking_from"] = parking_from
    g["parking_to"] = parking_to
    g["parking_duration_hours"] = parking_duration_hours
    g["hourly_price"] = hourly_price

    output.append(g)

# 4. Save JSON
out_file = "parkbee_garages.json"
with open(out_file, "w") as f:
    jso
