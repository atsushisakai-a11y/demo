import json
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

CET = ZoneInfo("Europe/Amsterdam")


# ------------------------------------------------------
# Helpers
# ------------------------------------------------------

def parse_datetime_to_cet(dt_str):
    """
    Convert a UTC datetime string to CET timezone.
    Supports formats like:
      - '2025-11-21T19:45:00.000Z'
      - 'Fri, 21 Nov 2025 14:28:40 GMT'
    """
    try:
        if "GMT" in dt_str:
            # Example: Fri, 21 Nov 2025 14:28:40 GMT
            dt = datetime.strptime(dt_str, "%a, %d %b %Y %H:%M:%S GMT")
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        else:
            # Example: 2025-11-21T19:45:00.000Z
            dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.astimezone(CET).isoformat()
    except Exception:
        return None


def extract_scrape_datetime(entry):
    """Extract scrape time from HTTP response headers."""
    headers = entry.get("response", {}).get("headers", [])
    for h in headers:
        if h.get("name", "").lower() == "date":
            return parse_datetime_to_cet(h["value"])
    return None


def extract_parking_window(entry):
    """
    Extract parking_from / parking_to from GraphQL request POST body.
    Looks in postData.text → "variables" → "pricing".
    """
    req = entry.get("request", {})
    post = req.get("postData", {})
    body = post.get("text")

    if not body:
        return None, None, None

    try:
        data = json.loads(body)
        vars_block = data.get("variables", {})
        pricing = vars_block.get("pricing", {})

        s = pricing.get("startDateTime")
        e = pricing.get("endDateTime")

        if not s or not e:
            return None, None, None

        start_cet = parse_datetime_to_cet(s)
        end_cet = parse_datetime_to_cet(e)

        duration_hours = None
        try:
            dt_start = datetime.fromisoformat(start_cet)
            dt_end = datetime.fromisoformat(end_cet)
            duration_hours = round((dt_end - dt_start).total_seconds() / 3600.0, 2)
        except:
            pass

        return start_cet, end_cet, duration_hours

    except Exception:
        return None, None, None


def extract_garages(entry):
    """Extract garage list from GraphQL response block."""
    resp = entry.get("response", {})
    content = resp.get("content", {})
    text = content.get("text")

    if not text:
        return None

    try:
        data = json.loads(text)
        return data.get("data", {}).get("searchGarages") or []
    except Exception:
        return None


# ------------------------------------------------------
# MAIN EXTRACTOR
# ------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python extract_parkbee.py input.har")
        sys.exit(1)

    har_file = sys.argv[1]

    with open(har_file, "r", encoding="utf8") as f:
        har = json.load(f)

    entries = har.get("log", {}).get("entries", [])

    all_output = []

    for entry in entries:
        garages = extract_garages(entry)
        if not garages:
            continue

        scrape_datetime = extract_scrape_datetime(entry)
        parking_from, parking_to, parking_duration_hours = extract_parking_window(entry)

        for g in garages:
            price_cost = (
                g.get("pricingAndAvailability", {})
                 .get("pricing", {})
                 .get("cost")
            )

            hourly_price = None
            if price_cost is not None and parking_duration_hours:
                try:
                    hourly_price = round(price_cost / parking_duration_hours, 2)
                except:
                    hourly_price = None

            all_output.append({
                "id": g.get("id"),
                "name": g.get("name"),
                "latitude": g.get("latitude"),
                "longitude": g.get("longitude"),
                "address": g.get("address"),
                "pricingAndAvailability": g.get("pricingAndAvailability"),
                "scrape_datetime": scrape_datetime,               # CET
                "parking_from": parking_from,                     # CET
                "parking_to": parking_to,                         # CET
                "parking_duration_hours": parking_duration_hours,
                "hourly_price": hourly_price
            })

    out_file = f"parkbee_garages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(out_file, "w", encoding="utf8") as f:
        for item in all_output:
            f.write(json.dumps(item) + "\n")

    print(f"📦 Extracted {len(all_output)} garages")
    print(f"📄 Output file: {out_file}")


if __name__ == "__main__":
    main()
