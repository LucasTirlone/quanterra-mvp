import requests
import json
import csv

# Configuration
CXY_API_TOKEN = "11089$iYTkhu3tUiJ9ZKtmJ9BlUetugSprAnV1Gvd3s7iD3U5Ho1muWaaIy9Zic0GQsxtgmnv58IYkTT0SVSuXTZlnExAJhzADRI8NkgOYCHZ3DqigW42fzFURiwOyoKQZUYU0"
BASE_URL = "https://location.chainxy.com"

headers = {
    "x-apikey": CXY_API_TOKEN,
    "x-application": "Python API Call",
    "content-type": "application/json",
}


def get_collection(collection_id):
    url = f"{BASE_URL}/api/ChainLists/{collection_id}"
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Error fetching collection {collection_id}: {response.status_code}")
        return None
    try:
        return response.json()
    except Exception as e:
        print(f"Error parsing JSON for collection {collection_id}: {e}")
        return None


def get_all_chains(chain_query):
    all_chains = []
    page = 0
    while True:
        url = f"{BASE_URL}/api/Chains"
        params = {
            "Query": json.dumps(chain_query),
            "Page": page
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error fetching chains on page {page}: {response.status_code}")
            break
        try:
            data = response.json()
        except Exception as e:
            print(f"Error parsing JSON on page {page}: {e}")
            break

        records = data.get("Records", [])
        all_chains.extend(records)
        if page >= data.get("Pages", 1) - 1:
            break
        page += 1
    return all_chains


def get_all_chain_scrapes(chain_id):
    all_records = []
    page = 0
    while True:
        url = f"{BASE_URL}/api/ChainScrapes"
        params = {
            "fields": "Id,RunDate,Status,LocationCount",
            "Query": json.dumps({"ChainId": chain_id}),
            "OrderBy": "RunDate",  # ✅ Sort oldest to newest
            "Page": page
            
        }
        response = requests.get(url, headers=headers, params=params)
        if response.status_code != 200:
            print(f"Error fetching chain scrapes for chain {chain_id} page {page}: {response.status_code}")
            break
        try:
            data = response.json()
        except Exception as e:
            print(f"Error parsing JSON for chain {chain_id} page {page}: {e}")
            break

        all_records.extend(data.get("Records", []))
        if page >= data.get("Pages", 1) - 1:
            break
        page += 1
    return all_records

def get_us_location_count(scrape_id):
    """
    Returns FilteredRecordCount for US + Open locations only.
    """
    url = f"{BASE_URL}/api/ScrapeLocations"
    query = {
        "ScrapeId": scrape_id,
        "ChainLocation/AdminLevel1Code": ["US"],
        "OpenStatus": ["Open"]
    }
    params = {
        "fields": "Id",
        "Query": json.dumps(query),
    }

    try:
        print(f"🔎 Fetching US OPEN count for ScrapeId {scrape_id}...")
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("FilteredRecordCount", 0)
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"⚠️ Error getting US OPEN count for ScrapeId {scrape_id}: {e}")
        return None

def main():
    collection_id = 303288  # ============ Replace with Collection ID ==========

    metadata = get_collection(collection_id)
    if not metadata:
        print("Failed to retrieve collection metadata.")
        return

    record = metadata.get("Record", {})
    chains_query_str = record.get("ChainsQuery")
    if not chains_query_str:
        print("No 'ChainsQuery' found in collection record.")
        return

    try:
        chain_query = json.loads(chains_query_str)
    except Exception as e:
        print("Error parsing ChainsQuery:", e)
        return

    chains = get_all_chains(chain_query)
    print(f"📦 Found {len(chains)} chains in collection {collection_id}.")

    output_rows = []
    for ch in chains:
        chain_id = ch.get("Id") or ch.get("ChainId")
        chain_name = ch.get("Name") or ch.get("DisplayValue") or f"Chain {chain_id}"
        if not chain_id:
            print("Chain missing ID, skipping.")
            continue

        print(f"\n🔍 Fetching scrapes for chain {chain_id} ({chain_name})...")
        scrapes = get_all_chain_scrapes(chain_id)
        if not scrapes:
            print(f"No scrapes found for chain {chain_id}.")
            continue

        for scrape in scrapes:
            run_datetime = scrape.get("RunDate", "")
            date_part, time_part = (run_datetime.split("T", 1) + [""])[:2]
            location_count = scrape.get("LocationCount")
            scrape_id = scrape.get("Id")
            us_location_count = get_us_location_count(scrape_id)

            output_rows.append({
                "ChainId": chain_id,
                "ChainName": chain_name,
                "date": date_part,
                "time": time_part,
                "locationcount": location_count,
                "us_locationcount": us_location_count
            })

    sorted_rows = sorted(output_rows, key=lambda r: (int(r["ChainId"]), r["date"]))

    # ======== CHANGE LOCATION ==============
    csv_filename = f"/foldername/Collection - Name/chainscrapes_collection_{collection_id}.csv"

    with open(csv_filename, mode="w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["ChainId", "ChainName", "date", "time", "locationcount", "us_locationcount"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in sorted_rows:
            writer.writerow(row)

    print(f"\n✅ CSV saved: {csv_filename} ({len(sorted_rows)} rows)")

if __name__ == "__main__":
    main()
