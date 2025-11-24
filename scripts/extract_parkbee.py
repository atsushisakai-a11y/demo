import json
import sys
from datetime import datetime, timezone

"""
ParkBee HAR → NDJSON Extractor
--------------------------------
Extracts garage objects from a HAR file and adds:

- parking_from
- parking_to
- parking_duration_hours
- hourly_price
- scrape_datetime

Output is written as newline-delimited JSON (NDJSON),
fully compatible with BigQuery ingestion.
"""


def parse_parking_window(har):
    """
    Scan entire HAR file for `searchGarages` request body
    and extract:
      - parking_from
      - parking_to
      - duration_hours
    """

    for entry in har.get("log", {}).get("entries", []):
        try:
            req = entry.get("request", {})
            res = entry.get("response", {})
            url = req.get("url", "")

            if "/graphql" not in url:
                continue

            # Extract request JSON
            post_data = req.get("postData", {}).get("text", "")
            if not post_data:
                continue

            body = json.loads(post_data)

            # Look for the ParkBee search query
            variables = body.get("variables", {})
            if "searchParams" in variables:
                params = variables["searchParams"]

                parking_from = params.get("from")
                parking_to = params.get("to")

                if parking_from and parking_to:
                    dt_from = datetime.fromisoformat(parking_from.replace("Z", "+00:00"))
                    dt_to = datetime.fromisoformat(parking_to.replace("Z", "+00:00"))

                    duration_hours = round((dt_to - dt_from).total_seconds() / 3600, 2)

                    return parking_from, parking_to, duration_hours

        except Exception:
            continue

    return None, None, None


def extract_garages(har):
    """
    Extract unique garages from HAR searchGarages response.
    """

    garages = {}

    for entry in har.get("log", {}).get("entries", []):
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

    return garages


def main():
    if len(sys.argv) != 2:
        print("Usage: python extract_parkbee.py <har_file>")
        sys.exit(1)

    har_file = sys.argv[1]

    with open(har_file, "r") as f:
        har = json.load(f)

    # ---- Extract parking window ----
    parking_from, parking_to, duration_hours = parse_parking_window(har)

    if parking_from:
        print(f"🕒 Parking window: {parking_from} → {parking_to} ({duration_hours}h)")
    else:
        print("⚠️ Could NOT detect parking_from / parking_to — setting NULL.")

    # ---- Extract garages ----
    garages = extract_garages(har)
    print(f"📦 Extracted {len(garages)} garages")

    # ---- Write NDJSON ----
    output_name = f"parkbee_garages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    scrape_datetime = datetime.now(timezone.utc).isoformat()

    with open(output_name, "w") as f:
        for g in garages.values():

            # If cost exists, compute hourly price
            try:
                total_cost = g["pricingAndAvailability"]["pricing"]["cost"]
                hourly_price = round(total_cost / duration_hours, 2) if duration_hours else None
            except:
                hourly_price = None

            g_out = {
                "id": g.get("id"),
                "name": g.get("name"),
                "latitude": g.get("latitude"),
                "longitude": g.get("longitude"),
                "address": g.get("address", {}),
                "pricingAndAvailability": g.get("pricingAndAvailability", {}),
                "scrape_datetime": scrape_datetime,

                # NEW FIELDS
                "parking_from": parking_from,
                "parking_to": parking_to,
                "parking_duration_hours": duration_hours,
                "hourly_price": hourly_price,
            }

            f.write(json.dumps(g_out) + "\n")

    print(f"💾 Saved NDJSON → {output_name}")


if __name__ == "__main__":
    main()
