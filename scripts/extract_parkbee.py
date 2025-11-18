import json
import sys

if len(sys.argv) != 2:
    print("Usage: python extract_parkbee.py garages.har")
    sys.exit(1)

har_file = sys.argv[1]

with open(har_file, "r") as f:
    har = json.load(f)

garages = []

for entry in har["log"]["entries"]:
    try:
        started_datetime = entry["startedDateTime"]  # extract scrape timestamp
        res = entry["response"]
        req = entry["request"]

        if "/graphql" not in req["url"]:
            continue

        content = res["content"].get("text", "")
        if not content or not content.startswith("{"):
            continue

        body = json.loads(content)
        data = body.get("data", {}).get("searchGarages", [])

        for garage in data:
            garage["scrape_datetime"] = started_datetime  # inject timestamp
            garages.append(garage)

    except Exception:
        continue

print(f"📦 Extracted {len(garages)} garages with scrape_datetime")

output_file = f"parkbee_garages_{garages[0]['scrape_datetime'][:10]}.json"
with open(output_file, "w") as f:
    for g in garages:
        f.write(json.dumps(g) + "\n")

print(f"💾 Saved → {output_file}")
