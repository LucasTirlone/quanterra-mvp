
import os
import requests

from email.utils import unquote
from fileinput import filename
from turtle import pd
from urllib.parse import urlparse


def __try_download_file(url: str, output_file: str = None):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()  # Ensure we handle HTTP errors

        with open(output_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def __get_output_file_path(url: str, download_folder: str) -> str:
    # Decode the URL to handle special characters (e.g., spaces %20 -> spaces)
    decoded_url = unquote(url)

    # Parse the URL to get the file name from the path part (not query parameters)
    parsed_url = urlparse(decoded_url)
    filename = os.path.basename(parsed_url.path)  # Get the file name from the URL path

    # Clean the file name by replacing invalid characters (e.g., colon or slash)
    filename = filename.replace(":", "_").replace("/", "_").replace(" ", "_")

    # Build the full path for the file
    return os.path.join(download_folder, filename)


def __create_folder_if_not_exists(folder: str):
    # Ensure the folder exists
    if not os.path.exists(folder):
        os.makedirs(folder)


def download_csv_file(url: str, output_file: str = None):
    file = download_file(url, output_file)

    if filename.endswith(".xlsx"):
        csv_file = os.path.splitext(output_file)[0] + ".csv"
        df = pd.read_excel(output_file)
        df.to_csv(csv_file, index=False)
        print(f"Converted XLSX to CSV: {csv_file}")
        return csv_file
    return file


def download_file(url: str, download_folder: str, output_file: str = None):
    """
    Downloads the file from the given URL and converts XLSX to CSV if needed.
    """
    if not url:
        print("No download URL provided.")
        return
    
    if not download_folder:
        print("No download folder provided.")
        return

    __create_folder_if_not_exists(download_folder)
    output_file = __get_output_file_path(url, download_folder)

    print(f"Attempting to save file to: {output_file}")
    print(f"Download URL: {url}")
    
    __try_download_file(url, output_file)

    # Check if the file was saved correctly
    if os.path.exists(output_file):
        print(f"File downloaded successfully to: {output_file}")
    else:
        print(f"Failed to download the file to: {output_file}")
    
    return output_file
