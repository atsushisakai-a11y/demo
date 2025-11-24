import json
import sys
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime


"""
ParkBee HAR → NDJSON extractor (FINAL VERSION)

Enhancements:
- Extracts parking window (start/end)
- Computes duration + hourly_price
- Extracts REAL scrape_datetime from response header "date"
- Writes NDJSON for BigQuery
"""


def parse_parking_window(har):
    """Search HAR for pricing window from variables.pricing / searchParams / bookingParams."""
    def extract_from_block(block):
        if not isinstance(block, dict):
            return None, None

        # Case 1 – variables.pricing.startDateTime / endDateTime
        pricing = block.get("pricing")
        if isinstance(pricing, dict):
            s = pricing.get("startDateTime")
            e = pricing.get("endDateTime")
            if s and e:
                return s, e

        # Case 2 – variables.searchParams.{from,to,startDateTime,endDateTime}
        sp = block.get("searchParams")
        if isinstance(sp, dict):
            s = sp.get("from") or sp.get("startDateTime")
            e = sp.get("to") or sp.get("endDateTime")
            if s and e:
                return s, e

        # Case 3 – variables.bookingParams.{from,to}
        bp = block.get("bookingParams")
        if isinstance(bp, dict):
            s = bp.get("from") or bp.get("startDateTime")
            e = bp.get("to") or bp.get("endDateTime")
            if s and e:
                return s, e

        return None, None

    for entry in har.get("log", {}).get("entries", []):
        try:
            req = entry.get("request", {})
            if "/graphql" not in req.get("url", ""):
                continue

            text = req.get("postData", {}).get("text", "")
            if not text or not text.startswith("{"):
                continue

            body = json.loads(text)
            variables = body.get("variables", {})
            blocks = [variables]

            # Also check nested operations
            if isinstance(variables.get("operations"), list):
                for op in variables["operations"]:
                    if isinstance(op, dict) and "variables" in op:
                        blocks.append(op["variables"])

            for block in blocks:
                s, e = extract_from_block(block)
                if s and e:
                    start_dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                    end_dt = datetime.fromisoformat(e.replace("Z", "+00:00"))
                    duration_hours = round((end_dt - start_dt).total_seconds() / 3600.0, 2)
                    return s, e, duration_hours

        except Exception:
            continue

    return None, None, None


def extract_scrape_datetime(entry):
    """Extract ISO8601 timestamp from response header 'date'."""
    headers = entry.get("response", {}).get("headers", [])
    for h in headers:
        if h.get("name", "").lower() == "date":
            try:
                dt = parsedate_to_datetime(h["value"])
                return dt.astimezone(timezone.utc).isoformat()
            except:
                return None
    return None


def main():
    if len(sys.argv) != 2:
        print("Usage: python extract_parkbee.py <har_file>")
        sys.exit(1)

    har_path = sys.argv[1]

    # Load HAR file
    with open(har_path, "r") as f:
        har = json.load(f)

    parking_from, parking_to, parking_duration_hours = parse_parking_window(har)

    if parking_from:
        print(f"🕒 Parking window found: {parking_from} → {parking_to} ({parking_duration_hours}h)")
    else:
        print("⚠️ No parking window found — parking_* fields will be NULL")

    garages = {}

    # Iterate through all HAR entries
    for entry in har.get("log", {}).get("entries", []):
        try:
            req = entry.get("request", {})
            res = entry.get("response", {})
            if "/graphql" not in req.get("url", ""):
                continue

            text = res.get("content", {}).get("text", "")
            if not text or not text.startswith("{"):
                continue

            body = json.loads(text)
            data = body.get("data", {})
            search_garages = data.get("searchGarages")

            if not isinstance(search_garages, list):
                continue

            # Extract real scrape_datetime from response
            scrape_dt = extract_scrape_datetime(entry)

            for g in search_garages:
                gid = g.get("id")
                if not gid:
                    continue

                # Compute hourly price
                pricing = g.get("pricingAndAvailability", {}).get("pricing", {})
                total_cost = pricing.get("cost")
                if parking_duration_hours and total_cost is not None:
                    try:
                        hourly_price = round(float(total_cost) / parking_duration_hours, 4)
                    except:
                        hourly_price = None
                else:
                    hourly_price = None

                garages[gid] = {
                    "id": gid,
                    "name": g.get("name"),
                    "latitude": g.get("latitude"),
                    "longitude": g.get("longitude"),
                    "address": g.get("address"),
                    "pricingAndAvailability": g.get("pricingAndAvailability"),
                    "scrape_datetime": scrape_dt,
                    "parking_from": parking_from,
                    "parking_to": parking_to,
                    "parking_duration_hours": parking_duration_hours,
                    "hourly_price": hourly_price,
                }

        except Exception:
            continue

    print(f"📦 Extracted {len(garages)} garages")

    # Save NDJSON
    outfile = f"parkbee_garages_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(outfile, "w") as f:
        for g in garages.values():
            f.write(json.dumps(g) + "\n")

    print(f"💾 Saved NDJSON → {outfile}")


if __name__ == "__main__":
    main()
