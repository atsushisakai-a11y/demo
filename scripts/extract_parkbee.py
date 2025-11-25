import json
import sys
from datetime import datetime, timezone, timedelta

CET = timezone(timedelta(hours=1))

har_file = sys.argv[1]

# Load HAR
with open(har_file, "r") as f:
    har = json.load(f)

entries = har.get("log", {}).get("entries", [])

garages = []
pricing_from = None
pricing_to = None
scrape_dt = None

# Extract pricing window + scrape datetime
for e in entries:
    req = e.get("request", {})
    post = req.get("postData", {})
    text = post.get("text")

    if text and "SearchGaragesQuery" in text:
        try:
            payload = json.loads(text)
            pricing = payload.get("variables", {}).get("pricing", {})
            pricing_from = pricing.get("startDateTime")
            pricing_to = pricing.get("endDateTime")
        except:
            pass

    # Extract scrape datetime from response headers
    resp = e.get("response", {})
    headers = resp.get("headers", [])
    for h in headers:
        if h.get("name", "").lower() == "date":
            raw_date = h.get("value")
            scrape_dt = datetime.strptime(raw_date, "%a, %d %b %Y %H:%M:%S GMT")
            scrape_dt = scrape_dt.replace(tzinfo=timezone.utc)
            break

# Convert window from ISO → datetime (CET → UTC)
def convert_iso_to_utc(iso):
    if iso is None:
        return None
    dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
    return dt.astimezone(timezone.utc).isoformat()

parking_from_utc = convert_iso_to_utc(pricing_from)
parking_to_utc = convert_iso_to_utc(pricing_to)

# Extract garage objects
for e in entries:
    resp = e.get("response", {})
    content = resp.get("content", {})

    if content.get("mimeType") == "application/json":
        try:
            data = json.loads(content.get("text", "{}"))
            if "searchGarages" in data:
                for g in data["searchGarages"]:
                    cost = g["pricingAndAvailability"]["pricing"]["cost"]
                    hours = None
                    hourly_price = None

                    if parking_from_utc and parking_to_utc:
                        dt_from = datetime.fromisoformat(parking_from_utc)
                        dt_to = datetime.fromisoformat(parking_to_utc)
                        hours = (dt_to - dt_from).total_seconds() / 3600
                        hourly_price = round(cost / hours, 2) if hours > 0 else None

                    garages.append({
                        "id": g.get("id"),
                        "name": g.get("name"),
                        "latitude": g.get("latitude"),
                        "longitude": g.get("longitude"),
                        "address": g.get("address"),
                        "pricingAndAvailability": g.get("pricingAndAvailability"),
                        "scrape_datetime": scrape_dt.isoformat() if scrape_dt else None,
                        "parking_from": parking_from_utc,
                        "parking_to": parking_to_utc,
                        "parking_duration_hours": hours,
                        "hourly_price": hourly_price
                    })
        except:
            continue

# Write NDJSON (NOT LIST)
timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f"parkbee_garages_{timestamp_str}.json"

with open(output_file, "w") as out:
    for g in garages:
        out.write(json.dumps(g) + "\n")

print(f"📦 Extracted {len(garages)} garages → {output_file}")
