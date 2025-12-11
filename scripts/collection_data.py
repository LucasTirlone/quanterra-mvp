import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from urllib.parse import unquote, urlparse
import json
import time

validate_url_api_key = "https://location.chainxy.com/api/Users/Me"  # This is the API endpoint to check if the API key is valid.

def check_api_key(CXY_API_TOKEN):
    """
    Validates the provided ChainXY API key.
    """
    headers = {
        "x-apikey": CXY_API_TOKEN,
        "x-application": "Python API Call",
        "content-type": "application/json",
    }

    # Send a request to validate the API key
    response = requests.get(validate_url_api_key, headers=headers)
    
    if response.status_code == 401:
        raise ValueError("Invalid API key provided. Please check the API key.")
    elif response.status_code != 200:
        raise ValueError(f"Error: {response.status_code} - {response.text}")

    print("API Key is valid.")


def download_changes_over_time_report(CXY_API_TOKEN, collection_id, report_params):
    """
    Download the Changes Over Time (COT) report in XLSX format.
    """
    check_api_key(CXY_API_TOKEN)
    headers = {
        "x-apikey": CXY_API_TOKEN,
        "x-application": "Python API Call",
        "content-type": "application/json",
    }

    # Requesting the report in XLSX format
    api_url = f"https://location.chainxy.com/api/ChainLists/ChangesOverTimeReport/{collection_id}?format=XLSX"
    response = requests.post(
        url=api_url, data=json.dumps(report_params), headers=headers
    )
    response.raise_for_status()
    response_body = json.loads(response.text)
    download_id = response_body["Id"]
    
    return check_report_status(CXY_API_TOKEN, download_id)


def check_report_status(CXY_API_TOKEN, download_id, check_interval_seconds=5):
    """
    Check the status of a report generation and return the report download URL.
    """
    headers = {
        "x-apikey": CXY_API_TOKEN,
        "x-application": "Python API Call",
        "content-type": "application/json",
    }

    download_link = False
    generated_report_link = None

    while not download_link:
        status_url = f"https://location.chainxy.com/api/Downloads/{download_id}"
        response = requests.get(url=status_url, headers=headers)
        record = json.loads(response.text)["Record"]

        if record["Status"] == 0:
            print(f"Download for report {download_id} is still generating.")
            time.sleep(check_interval_seconds)
        elif record["Status"] == 2:
            print("Report generation failed. Contact ChainXY for assistance.")
            download_link = True
        elif record["Status"] == 1:
            print("Report generation completed!")
            download_link = True
            generated_report_link = record["Link"]

    return generated_report_link


def download_file(url: str, output_file: str = None):
    """
    Downloads the file from the given URL and converts XLSX to CSV if needed.
    """
    if not url:
        print("No download URL provided.")
        return

    # Decode the URL to handle special characters (e.g., spaces %20 -> spaces)
    decoded_url = unquote(url)

    # Parse the URL to get the file name from the path part (not query parameters)
    parsed_url = urlparse(decoded_url)
    filename = os.path.basename(parsed_url.path)  # Get the file name from the URL path

    # Define the folder path where files should be saved CHANGE THIS FOR EACH COLLECTION
    download_folder = '/Users/officemini/Library/CloudStorage/OneDrive-QuanterraResearch/Work/Quanterra Buildout/CXY Data Downloads/Collection - US Pet Stores/PetSmart download test'
    
    # Ensure the folder exists
    if not os.path.exists(download_folder):
        os.makedirs(download_folder)

    # Clean the file name by replacing invalid characters (e.g., colon or slash)
    filename = filename.replace(":", "_").replace("/", "_").replace(" ", "_")

    # Build the full path for the file
    output_file = os.path.join(download_folder, filename)

    print(f"Attempting to save file to: {output_file}")
    
    # Debug: Print the actual URL being used for downloading
    print(f"Download URL: {parsed_url}")
    
    # Try downloading the file
    with requests.get(url, stream=True) as r:
        r.raise_for_status()  # Ensure we handle HTTP errors

        with open(output_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    # Check if the file was saved correctly
    if os.path.exists(output_file):
        print(f"File downloaded successfully to: {output_file}")

        # Convert XLSX to CSV if the file is in XLSX format
        if filename.endswith(".xlsx"):
            csv_file = os.path.splitext(output_file)[0] + ".csv"
            df = pd.read_excel(output_file)
            df.to_csv(csv_file, index=False)
            print(f"Converted XLSX to CSV: {csv_file}")
            return csv_file  # Return the CSV file path for later use
    else:
        print(f"Failed to download the file to: {output_file}")

    return output_file  # Return the XLSX file path if no conversion was done


def generate_reports_in_intervals(CXY_API_TOKEN, collection_id, start_date, end_date):
    """
    Generate reports for each 30-day interval between start_date and end_date.
    """
    # Ensure that both start_date and end_date are offset-aware datetimes
    if start_date.tzinfo is None:
        start_date = start_date.replace(tzinfo=timezone.utc)
    if end_date.tzinfo is None:
        end_date = end_date.replace(tzinfo=timezone.utc)

    def format_as_date(dt):
        return dt.strftime("%Y-%m-%d")
    
    current_date = start_date

    while current_date < end_date:
        # Define the start and end dates for the report
        report_start_date = current_date
        report_end_date = current_date + timedelta(days=30)

        # Ensure that the report_end_date doesn't exceed the overall end_date
        if report_end_date > end_date:
            report_end_date = end_date
        
        print(f"Generating report from {report_start_date} to {report_end_date}...")

        # Parameters for the Changes Over Time Report
        changes_over_time_report_params = {
            "IncludeSummaryStats": False,
            "StartDate": format_as_date(report_start_date),
            "EndDate": format_as_date(report_end_date),
            "IncludeChangeLog": True,
            "IncludeCountByOpenStatus": False,
            "IncludeCountByCountry": False,
            "IncludeCountByState": False,
            "IncludeCountByStoreType": False,
            "IncludeCountByDMA": False,
            "IncludeCountByCounty": False,
        }

        # Download and convert the report for the 30-day period
        report_file_url = download_changes_over_time_report(
            CXY_API_TOKEN, collection_id, changes_over_time_report_params
        )

        print(f"Download URL received: {report_file_url}")

        downloaded_file = download_file(report_file_url)
        
        print(f"Report downloaded and converted to CSV: {downloaded_file}")

        # Move to the next 30-day period
        current_date = report_end_date


def main():
    CXY_API_TOKEN = "token_goes_here"  # CHANGE to your ChainXY API token
    collection_id = 303288  # CHANGE the collection ID for EACH COLLECTION

    # Specify the start and end date
    start_date = datetime(2017, 1, 1)  # Example: first scraped data date
    end_date = datetime.now(timezone.utc)

    # Generate reports in 30-day increments from start_date to end_date
    generate_reports_in_intervals(CXY_API_TOKEN, collection_id, start_date, end_date)


if __name__ == "__main__":
    main()
