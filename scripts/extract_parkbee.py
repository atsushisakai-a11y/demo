import json
import sys
from datetime import datetime, timezone

"""
ParkBee HAR → NDJSON extractor

- Scans HAR for GraphQL requests to /graphql
- Extracts parking window (start/end datetime) from:
    variables.pricing.startDateTime / endDateTime
  and also supports searchParams / bookingParams variants.
- Extracts garages from SearchGaragesQuery responses
- Adds:
    parking_from
    parking_to
    parking_duration_hours
    hourly_price
    scrape_datetime
- Outputs NDJSON (one JSON object per line), BigQuery compatible.
"""


def parse_parking_window(har):
    """
    Look through HAR for the GraphQL request that contains the parking window.
    Supports:
      - variables.pricing.startDateTime / endDateTime
      - variables.searchParams.{from,to,startDateTime,endDateTime}
      - variables.bookingParams.{from,to,startDateTime,endDateTime}
      - nested variables.operations[*].variables with the same patterns
    Returns:
      (parking_from_iso, parking_to_iso, parking_duration_hours) or (None, None, None)
    """

    def extract_from_block(block):
        """Try all known patterns inside a single variables block."""
        if not isinstance(block, dict):
            return None, None

        # 1) variables.pricing.startDateTime / endDateTime
        pricing = block.get("pricing")
        if isinstance(pricing, dict):
            s = pricing.get("startDateTime")
            e = pricing.get("endDateTime")
            if s and e:
                return s, e

        # 2) variables.searchParams.{from,to,startDateTime,endDateTime}
        sp = block.get("searchParams")
        if isinstance(sp, dict):
            s = sp.get("from") or sp.get("startDateTime")
            e = sp.get("to") or sp.get("endDateTime")
            if s and e:
                return s, e

        # 3) variables.bookingParams.{from,to,startDateTime,endDateTime}
        bp = block.get("bookingParams")
        if isinstance(bp, dict):
            s = bp.get("from") or bp.get("startDateTime")
            e = bp.get("to") or bp.get("endDateTime")
            if s and e:
                return s, e

        return None, None

    # Walk all HAR entries
    for entry in har.get("log", {}).get("entries", []):
        try:
            req = entry.get("request", {})
            url = req.get("url", "")
            if "/graphql" not in url:
                continue

            text = req.get("postData", {}).get("text", "")
            if not text or not text.startswith("{"):
                continue

            body = json.loads(text)
            variables = body.get("variables", {})

            candidate_blocks = [variables]

            # Also check nested operations if present
            ops = variables.get("operations")
            if isinstance(ops, list):
                for op in ops:
                    if isinstance(op, dict) and "variables" in op:
                        candidate_blocks.append(op["variables"])

            for block in candidate_blocks:
                start_iso, end_iso = extract_from_block(block)
                if start_iso and end_iso:
                    # Compute duration in hours
                    start_dt = datetime.fromisoformat(start_iso.replace("Z", "+00:00"))
                    end_dt = datetime.fromisoformat(end_iso.replace("Z", "+00:00"))
                    duration_hours = round((end_dt - start_dt).total_seconds() / 3600.0, 2)
                    return start_iso, end_iso, duration_hours

        except Exception:
            # Ignore entry-level parsing issues
            continue

    # If nothing found
    return None, None, None


def main():
    # ----------------- CLI arg -----------------
    if len(sys.argv) != 2:
        print("Usage: python extract_parkbee.py <har_file>")
        sys.exit(1)

    har_path = sys.argv[1]

    # ----------------- Load HAR -----------------
    with open(har_path, "r") as f:
        har = json.load(f)

    # ----------------- Parking window -----------------
    parking_from, parking_to, parking_duration_hours = parse_parking_window(har)

    if parking_from and parking_to:
        print(
            f"🕒 Parking window: {parking_from} → {parking_to} "
            f"({parking_duration_hours} hours)"
        )
    else:
        print("⚠️ No parking window detected; leaving parking_* fields NULL.")

    # ----------------- Extract garages -----------------
    garages = {}

    for entry in har.get("log", {}).get("entries", []):
        try:
            req = entry.get("request", {})
            res = entry.get("response", {})
            url = req.get("url", "")

            if "/graphql" not in url:
                continue

            text = res.get("content", {}).get("text", "")
            if not text or not text.startswith("{"):
                continue

            body = json.loads(text)
            data = body.get("data", {})
            search_garages = data.get("searchGarages")
            if not isinstance(search_garages, list):
                continue

            for g in search_garages:
                gid = g.get("id")
                if not gid:
                    continue

                pricing = (
                    g.get("pricingAndAvailability", {})
                     .get("pricing", {})
                )
                total_cost = pricing.get("cost")

                if parking_duration_hours and total_cost is not None:
                    try:
                        hourly_price = round(float(total_cost) / parking_duration_hours, 4)
                    except Exception:
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
                    "scrape_datetime": datetime.now(timezone.utc).isoformat(),
                    "parking_from": parking_from,
                    "parking_to": parking_to,
                    "parking_duration_hours": parking_duration_hours,
                    "hourly_price": hourly_price,
                }

        except Exception:
            # Ignore broken entries but don't stop the whole run
            continue

    print(f"📦 Extracted {len(garages)} garages")

    # ----------------- Save NDJSON -----------------
    output_file = f"parkbee_garages_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
    with open(output_file, "w") as f:
        for g in garages.values():
            f.write(json.dumps(g) + "\n")

    print(f"💾 Saved NDJSON → {output_file}")


if __name__ == "__main__":
    main()
