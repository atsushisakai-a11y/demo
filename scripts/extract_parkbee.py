import json
import sys
import re
from datetime import datetime, timezone, timedelta

# -----------------------------------------
# Convert ISO8601 with offset → UTC ISO-8601 Z format
# -----------------------------------------
def to_utc(dt_str):
    if not dt_str:
        return None

    # Convert "Z" → "+00:00" so datetime.fromisoformat can parse
    if dt_str.endswith("Z"):
        dt_str = dt_str.replace("Z", "+00:00")

    dt = datetime.fromisoformat(dt_str)  # includes offset
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.isoformat().replace("+00:00", "Z")


# -----------------------------------------
# CLI arg
# -----------------------------------------
if len(sys.argv) != 2:
    print("Usage: python extract_parkbee.py file.har")
    sys.exit(1)

har_file = sys.argv[1]

with open(har_file, "r") as f:
    har = json.load(f)

garages = {}
parking_from = None
parking_to = None
scrape_datetime = None

# -----------------------------------------
# STEP 1 — Extract parking_from / parking_to from GraphQL pricing
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
    pricing = variables.get("pricing", {})

    pf = pricing.get("startDateTime")
    pt = pricing.get("endDateTime")

    if pf and pt:
        parking_from = pf
        parking_to = pt
        break

# -----------------------------------------
# STEP 2 — Extract scrape_datetime from response headers["date"]
# -----------------------------------------
for entry in har["log"]["entries"]:
    resp = entry.get("response", {})
    headers = resp.get("headers", [])

    for h in headers:
        if h.get("name", "").lower() == "date":
            try:
                # "Fri, 21 Nov 2025 14:28:40 GMT"
                dt = datetime.strptime(h["value"], "%a, %d %b %Y %H:%M:%S GMT")
                dt = dt.replace(tzinfo=timezone.utc)
                scrape_datetime = dt.isoformat().replace("+00:00", "Z")
            except:
                pass

    if scrape_datetime:
        break

# Fallback
if not scrape_datetime:
    scrape_datetime = datetime.utcnow().replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

# Convert parking times → UTC
pf_utc = to_utc(parking_from)
pt_utc = to_utc(parking_to)

# Calculate duration
parking_duration_hours = None
hourly_price = None

if pf_utc and pt_utc:
    dt1 = datetime.fromisoformat(pf_utc.replace("Z", "+00:00"))
    dt2 = datetime.fromisoformat(pt_utc.replace("Z", "+00:00"))
    hours = (dt2 - dt1).total_seconds() / 3600
    parking_duration_hours = round(hours, 2)


# -----------------------------------------
# STEP 3 — Extract garages
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

        cost = None
        try:
            cost = g["pricingAndAvailability"]["pricing"]["cost"]
        except:
            pass

        item = {
            "id": gid,
            "name": g.get("name"),
            "latitude": g.get("latitude"),
            "longitude": g.get("longitude"),
            "address": g.get("address"),
            "pricingAndAvailability": g.get("pricingAndAvailability"),
            "scrape_datetime": scrape_datetime,
            "parking_from": pf_utc,
            "parking_to": pt_utc,
            "parking_duration_hours": parking_duration_hours,
            "hourly_price": round(cost / parking_duration_hours, 2)
                               if cost and parking_duration_hours else None
        }

        garages[gid] = item


# -----------------------------------------
# Output
# -----------------------------------------
output_name = f"parkbee_garages_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
with open(output_name, "w") as f:
    json.dump(list(garages.values()), f, indent=2)

print(f"📦 Extracted {len(garages)} garages")
print(f"💾 Saved → {output_name}")
