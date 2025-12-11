import logging
import json
import requests
import time

from datetime import timedelta
from utils.api_cxy_util import get_cxy_report_status_url, get_cxy_report_url, get_cxy_validate_api_key_url, get_cxy_headers
from utils.data_util import format_as_date
from utils.download_util import download_csv_file


def generate_reports_weekly(collection_id, start_date):
    return generate_reports_in_intervals(collection_id, start_date, start_date + timedelta(weeks=1))


def generate_reports_in_intervals(collection_id, start_date, end_date, folder, file_name):        
    logging.info(f"Generating report from {start_date} to {end_date}...")

    changes_over_time_report_params = __get_changes_over_time_report_params(start_date, end_date)
    report_file_url = download_changes_over_time_report_url(collection_id, changes_over_time_report_params)

    logging.info(f"Download URL received: {report_file_url}")

    downloaded_file = download_csv_file(report_file_url, folder, file_name)
        
    logging.info(f"Report downloaded and converted to CSV: {downloaded_file}")
    return downloaded_file


def download_changes_over_time_report_url(collection_id, report_params):
    check_api_key()
    api_url = get_cxy_report_url(collection_id, "XLSX")
    
    response = requests.post(url=api_url, data=json.dumps(report_params), headers=get_cxy_headers())
    response.raise_for_status()

    response_body = json.loads(response.text)
    download_id = response_body["Id"]
    
    return check_report_status_and_get_url(download_id)


def check_api_key():
    # Send a request to validate the API key
    response = requests.get(get_cxy_validate_api_key_url(), headers=get_cxy_headers())
    
    if response.status_code == 401:
        raise ValueError("Invalid API key provided. Please check the API key.")
    elif response.status_code != 200:
        raise ValueError(f"Error: {response.status_code} - {response.text}")

    logging.info("API Key is valid.")


def check_report_status_and_get_url(download_id, check_interval_seconds=5):
    download_link = False
    generated_report_link = None

    while not download_link:
        status_url = get_cxy_report_status_url(download_id)
        response = requests.get(url=status_url, headers=get_cxy_headers())
        record = json.loads(response.text)["Record"]

        if record["Status"] == 0:
            logging.info(f"Download for report {download_id} is still generating.")
            time.sleep(check_interval_seconds)
        elif record["Status"] == 2:
            logging.info("Report generation failed. Contact ChainXY for assistance.")
            download_link = True
        elif record["Status"] == 1:
            logging.info("Report generation completed!")
            download_link = True
            generated_report_link = record["Link"]

    return generated_report_link


def __get_changes_over_time_report_params(start_date, end_date):
    return {
        "IncludeSummaryStats": False,
        "StartDate": format_as_date(start_date),
        "EndDate": format_as_date(end_date),
        "IncludeChangeLog": True,
        "IncludeCountByOpenStatus": False,
        "IncludeCountByCountry": False,
        "IncludeCountByState": False,
        "IncludeCountByStoreType": False,
        "IncludeCountByDMA": False,
        "IncludeCountByCounty": False,
    }
