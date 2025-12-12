import logging
import requests
import json
import csv

from utils.api_cxy_util import get_cxy_chain_scrapes_params, get_cxy_chain_scrapes_url, get_cxy_chains_params, get_cxy_chains_url, get_cxy_collection_url, get_cxy_headers, get_cxy_scrape_locations_params, get_cxy_scrape_locations_url

fieldnames = ['ChainId','ChainName','Date','Time','LocationCount','UsLocationCount']


def generate_chain_scrape_in_intervals(collection_id, start_date, end_date, folder, file_name):
    chain_scrapes = get_output_rows(collection_id, start_date, end_date)
    if "error" in chain_scrapes:
        raise RuntimeError(f"Error generating chain scrapes: {chain_scrapes['error']}")
    
    __save_csv(f"{folder}/{file_name}", chain_scrapes["rows"]),


def get_output_rows(collection_id, start_date, end_date):
    try:
        metadata = __get_collection(collection_id)
        chain_query = __get_chain_query(metadata)

        chains = __get_all_chains(chain_query)
        logging.info(f"Found {len(chains)} chains in collection {collection_id}.")

        output_rows, not_found_chain_id_count = __get_rows_from_chains(chains, start_date, end_date)
        sorted_rows = sorted(output_rows, key=lambda r: (int(r["ChainId"]), r["Date"]))

        logging.info(f"CSV saved: ({len(sorted_rows)} rows)")
        logging.info(f"Chains skipped due to missing ChainId: {not_found_chain_id_count}")
        return {
            "rows": sorted_rows,
            "not_found_chain_id_count": not_found_chain_id_count
        }
    except Exception as e:
        logging.error(e)
        return { "error": str(e)}


def __get_rows_from_chains(chains, start_date, end_date):
    output_rows = []
    not_found_chain_id_count = 0
    
    for ch in chains:
        chain_id = ch.get("Id") or ch.get("ChainId")
        chain_name = ch.get("Name") or ch.get("DisplayValue") or f"Chain {chain_id}"
        if not chain_id:
            logging.info("Chain missing ID, skipping.")
            not_found_chain_id_count += 1
            continue

        logging.info(f"Fetching scrapes for chain {chain_id} ({chain_name})...")
        scrapes = __get_all_chain_scrapes(chain_id, start_date, end_date)
        if not scrapes:
            logging.info(f"No scrapes found for chain {chain_id}.")
            continue

        output_rows.extend(__get_rows_from_scrapes(chain_id, chain_name, scrapes))

    return output_rows, not_found_chain_id_count


def __get_rows_from_scrapes(chain_id, chain_name, scrapes):
    rows = []
    for scrape in scrapes:
        run_datetime = scrape.get("RunDate", "")
        date_part, time_part = (run_datetime.split("T", 1) + [""])[:2]
        location_count = scrape.get("LocationCount")
        scrape_id = scrape.get("Id")
        us_location_count = __get_us_location_count(scrape_id)

        rows.append({
            "ChainId": chain_id,
            "ChainName": chain_name,
            "Date": date_part,
            "Time": time_part,
            "LocationCount": location_count,
            "UsLocationCount": us_location_count
        })
    return rows


def __save_collection_csv(collection_id, rows):
    csv_filename = f"/foldername/Collection - Name/chainscrapes_collection_{collection_id}.csv"
    fieldnames = ["ChainId", "ChainName", "date", "time", "locationcount", "us_locationcount"]
    try:
        with open(csv_filename, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    except Exception as e:
        logging.error(f"Error saving CSV file: {e}")


def __get_chain_query(metadata):
    try:
        record = metadata.get("Record", {})
        chains_query_str = record.get("ChainsQuery")
        return json.loads(chains_query_str)
    except Exception as e:
        raise RuntimeError("Error parsing ChainsQuery from collection metadata: " + str(e))


def __get_collection(collection_id):
    url = get_cxy_collection_url(collection_id)
    return __get_cxy_response_for_url(url, "Collection")
    

def __get_all_chains(chain_query):
    url = get_cxy_chains_url()
    params = get_cxy_chains_params(chain_query)
    return __get_cxy_response_for_url_with_pages(url, "Chains", params=params)


def __get_all_chain_scrapes(chain_id, start_date, end_date):
    url = get_cxy_chain_scrapes_url()
    params = get_cxy_chain_scrapes_params(chain_id, start_date, end_date)
    return __get_cxy_response_for_url_with_pages(url, "Chain Scrapes", params=params)


def __get_us_location_count(scrape_id):
    url = get_cxy_scrape_locations_url()
    params = get_cxy_scrape_locations_params(scrape_id)
    data = __get_cxy_response_for_url(url, "Scrape Locations", params=params, timeout=30)
    return data.get("FilteredRecordCount", 0)


def __get_cxy_response_for_url_with_pages(url, type, params=None):
    all_records = []
    page = 0
    while True:
        params['Page'] = page
        data = __get_cxy_response_for_url(url, type, params=params)

        if data is None:
            break

        all_records.extend(data.get("Records", []))
        if page >= data.get("Pages", 1) - 1:
            break
        page += 1
    return all_records


def __get_cxy_response_for_url(url, type, params=None, timeout=None):
    response = requests.get(url, headers=get_cxy_headers(), params=params, timeout=timeout)
    if timeout is not None:
        response.raise_for_status()

    if response.status_code != 200:
        return RuntimeError(f"[{type}] Error fetching URL {url} with params {params}: StatusCode-{response.status_code}")
    try:
        return response.json()
    except Exception as e:
        return RuntimeError(f"[{type}] Error parsing JSON for URL {url} with params {params}: Error: {e}")


def __save_csv(file_name, rows):
    try:
        with open(file_name, mode='w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
    except Exception as e:
        logging.error(f"Error saving CSV file: {e}")