import json
import sys
from datetime import datetime, timezone, timedelta

# -------------- CET CONFIG --------------
# Central European Time (winter +1, summer +2)
CET = timezone(timedelta(hours=1))  # NOVEMBER is winter time

# ----------------------------------------
#               HELPERS
# ----------------------------------------

def to_cet(dt_str):
    """
    Convert UTC datetime string into CET datetime, preserving correct offset.
    """
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        dt_cet = dt.astimezone(CET)
        return dt_cet.isoformat()
    except:
        return None


def extract_datetime_from_response(entry):
    """
    Extract scrape timestamp from response.headers.date
    """
    try:
        hdrs = entry["response"]["headers"]
        for h in hdrs:
            if h["name"].lower() == "date":
                dt = datetime.strptime(h["value"], "%a, %d %b %Y %H:%M:%S %Z")
                dt_cet = dt.replace(tzinfo=timezone.utc).astimezone(CET)
                return dt_cet.isoformat()
    except:
        pass
    return None


def extract_pricing_window(post_data_json):
    """
    Extract pricing.startDateTime & pricing.endDateTime from GraphQL variables.
    """
    try:
        vars_ = post_data_json.get("variables", {})
        pricing = vars_.get("pricing", {})
        start_raw = pricing.get("startDateTime")
        end_raw = pricing.get("endDateTime")
        if not start_raw or not end_raw:
            return None, None

        start_cet = to_cet(start_raw)
        end_cet = to_cet(end_raw)
        return start_cet, end_cet
    except:
        return None, None


def extract_garages(post_data_json):
    """
    Extract array of garages from GraphQL response payload inside the HAR entry.
    """
    try:
        data = post_data_json.get("data", {})
        garages = data.get("searchGarages", [])
        return garages
    except:
        return []


def calculate_hours(start_iso, end_iso):
    try:
        s = datetime.fromisoformat(start_iso)
        e = datetime.fromisoformat(end_iso)
        return (e - s).total_seconds() / 3600
    except:
        return None


# ----------------------------------------
#               MAIN
# ----------------------------------------

if len(sys.argv) < 2:
    print("Usage: python extract_parkbee.py <har_file>")
    sys.exit(1)

har_file = sys.argv[1]

with open(har_file, "r", encoding="utf8") as f:
    har = json.load(f)

entries = har["log"]["entries"]

all_garages = []
seen_ids = set()

for entry in entries:
    # Only POST requests with JSON body
    if "postData" not in entry["request"]:
        continue
    if "text" not in entry["request"]["postData"]:
        continue

    raw_text = entry["request"]["postData"]["text"]

    try:
        request_json = json.loads(raw_text)
    except:
        continue

    # Extract CET scrape timestamp
    scrape_datetime_cet = extract_datetime_from_response(entry)

    # Extract pricing window (CET)
    parking_from, parking_to = extract_pricing_window(request_json)

    # Look for response body
    if "content" not in entry["response"]:
        continue
    content = entry["response"]["content"]
    if "text" not in content:
        continue

    # GraphQL response JSON
    try:
        response_json = json.loads(content["text"])
    except:
        continue

    garages = extract_garages(response_json)

    for g in garages:
        gid = g.get("id")
        if not gid or gid in seen_ids:
            continue
        seen_ids.add(gid)

        cost = g.get("pricingAndAvailability", {}).get("pricing", {}).get("cost")
        duration = calculate_hours(parking_from, parking_to) if parking_from and parking_to else None
        hourly_price = None

        if cost is not None and duration:
            hourly_price = round(cost / duration, 2)

        all_garages.append({
            "id": gid,
            "name": g.get("name"),
            "latitude": g.get("latitude"),
            "longitude": g.get("longitude"),
            "address": {
                "city": g.get("address", {}).get("city"),
                "country": g.get("address", {}).get("country")
            },
            "pricingAndAvailability": g.get("pricingAndAvailability"),
            "scrape_datetime": scrape_datetime_cet,
            "parking_from": parking_from,
            "parking_to": parking_to,
            "parking_duration_hours": duration,
            "hourly_price": hourly_price
        })

# ----------------------------------------
#         WRITE NDJSON OUTPUT
# ----------------------------------------

output_file = f"parkbee_garages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
with open(output_file, "w", encoding="utf8") as f:
    for row in all_garages:
        f.write(json.dumps(row) + "\n")

print(f"✅ Extracted {len(all_garages)} garages")
print(f"📄 Output written to {output_file}")
