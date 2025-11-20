import json
import os


CXY_API_TOKEN = os.getenv('PARTNER_TOKEN')
BASE_URL = "https://location.chainxy.com/api"


def get_cxy_headers():
    return  {
        "x-apikey": CXY_API_TOKEN,
        "x-application": "Python API Call",
        "content-type": "application/json",
    }

def get_cxy_validate_api_key_url():
    return f"{BASE_URL}/Users/Me"


def get_cxy_report_url(collection_id, format):
    return f"{BASE_URL}/ChainLists/ChangesOverTimeReport/{collection_id}?format={format}"


def get_cxy_report_status_url(download_id):
    return f"{BASE_URL}/Downloads/{download_id}"


def get_cxy_collection_url(collection_id):
    return f"{BASE_URL}/ChainLists/{collection_id}"


def get_cxy_chains_url():
    return f"{BASE_URL}/Chains"


def get_cxy_chains_params(chain_query, page):
    return {
        "Query": json.dumps(chain_query),
        "Page": page
    }


def get_cxy_chain_scrapes_url():
    return f"{BASE_URL}/ChainScrapes"


def get_cxy_chain_scrapes_params(chain_id, page=0):
    return {
        "fields": "Id,RunDate,Status,LocationCount",
        "Query": json.dumps({"ChainId": chain_id}),
        "OrderBy": "RunDate",  # ✅ Sort oldest to newest
        "Page": page
    }

def get_cxy_scrape_locations_url():
    return f"{BASE_URL}/ScrapeLocations"


def get_cxy_scrape_locations_params(scrape_id):
    query = {
        "ScrapeId": scrape_id,
        "ChainLocation/AdminLevel1Code": ["US"],
        "OpenStatus": ["Open"]
    }
    return {
        "fields": "Id",
        "Query": json.dumps(query),
    }