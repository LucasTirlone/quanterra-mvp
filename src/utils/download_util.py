
import os
import requests

from email.utils import unquote
from fileinput import filename
import pandas as pd
from urllib.parse import urlparse


def __try_download_file(url: str, output_file: str = None):
    with requests.get(url, stream=True) as r:
        r.raise_for_status()  # Ensure we handle HTTP errors

        with open(output_file, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def __get_output_file_path(download_folder: str, filename: str) -> str:
    return os.path.join(download_folder, filename)


def __create_folder_if_not_exists(folder: str):
    # Ensure the folder exists
    if not os.path.exists(folder):
        os.makedirs(folder)


def download_csv_file(url: str, download_folder: str, filename: str):
    file = download_file(url, download_folder, filename)

    if filename.endswith(".xlsx"):
        csv_file = os.path.splitext(file)[0] + ".csv"
        df = pd.read_excel(file)
        df.to_csv(csv_file, index=False)
        print(f"Converted XLSX to CSV: {csv_file}")
        return csv_file
    return file


def download_file(url: str, download_folder: str, filename: str):
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
    output_file = __get_output_file_path(download_folder, filename)

    print(f"Attempting to save file to: {output_file}")
    print(f"Download URL: {url}")
    
    __try_download_file(url, output_file)

    # Check if the file was saved correctly
    if os.path.exists(output_file):
        print(f"File downloaded successfully to: {output_file}")
    else:
        print(f"Failed to download the file to: {output_file}")
    
    return output_file
