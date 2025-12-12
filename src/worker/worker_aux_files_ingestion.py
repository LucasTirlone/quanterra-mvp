from importlib.resources import files
import logging
import os
import boto3
import pandas as pd

from sqs.base_sqs_consumer import BaseSQSConsumer
from service.s3 import S3CsvService
from partner.chain_scrapes_api import generate_chain_scrape_in_intervals
from partner.collection_api import generate_reports_in_intervals
from service.file_event_service import create_file_event_log_for_uploaded, create_file_event_log_for_error
from service.db_service import get_db_session

from datetime import datetime, timedelta

from src.service.center_service import update_centers_from_excel
from src.service.landlord_service import upsert_landlords_from_excel
from src.service.location_service import update_location_status
from src.service.parent_chain_service import upsert_parent_chains_from_excel
from src.service.quality_report_service import generate_quality_report_and_save
from src.service.report_service import generate_report_for_chain_scraper, generate_report_for_collection
from src.service.us_region_service import update_regions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

sqs = boto3.client("sqs", region_name="us-east-1")

collection_id = 303288
session = get_db_session()

s3_raw_bucket = os.getenv("S3_RAW_BUCKET_NAME")
s3_processed_bucket = os.getenv("S3_PROCESSED_BUCKET_NAME")


class ReportGenerationConsumer(BaseSQSConsumer):
    queue_name = os.getenv("SQS_QUEUE_FILE_INGESTION")
    
    resp = sqs.get_queue_url(QueueName=queue_name)
    queue_url = resp["QueueUrl"]

    def handle(self, payload, message_attributes, raw_message):
        current_file_key = ""
        now = datetime.now().date()

        try:
            folder = "./download/aux-files/"
            
            files_key = S3CsvService.list_csv_files("curated", bucket_name=s3_raw_bucket)
            if not files_key:
                return
            
            for file_key in files_key:
                current_file_key = file_key
                file = S3CsvService.download_csv_file(current_file_key, "curated", folder, bucket_name=s3_raw_bucket)
                if not file:
                    continue        
                
                if file_key.startswith("Table - US Regions"):
                    df_xlsl = pd.read_excel(file)
                    update_regions(session, df_xlsl)
                elif file_key.startswith("Table - Parent Chains"):
                    df_xlsl = pd.read_excel(file)
                    upsert_parent_chains_from_excel(session, df_xlsl)
                elif file_key.startswith("Table - Centers"):
                    df_xlsl = pd.read_excel(file)
                    update_centers_from_excel(session, df_xlsl)
                elif file_key.startswith("Table - Landlords"):
                    df_xlsl = pd.read_excel(file)
                    upsert_landlords_from_excel(session, df_xlsl)
                else:
                    raise ValueError(f"Unrecognized auxiliary file key: {file_key}")
                    
                S3CsvService.move_files(
                    bucket_name=s3_raw_bucket,
                    source_folder="curated/",
                    destination_folder="curated-processed/",
                    file_keys=[current_file_key],
                    dry_run=False
                )
                S3CsvService.clean_local_files(folder, [file_key])
                
                create_file_event_log_for_uploaded(session, current_file_key, None, now)
        
        except Exception as error:
            create_file_event_log_for_error(session, current_file_key, None, now, "AUX_FILES_INGESTION", error)
    
    
    def __get_file_key_info(self, file_key):
        file_key_parts = file_key.replace(".csv", "").split("_")
        
        if len(file_key_parts) != 6:
            raise ValueError(f"Invalid file key format: {file_key}")
        
        try:            
            file_type = file_key_parts[0]
            collection_id = file_key_parts[1]
            start_scraper_date = datetime.strptime(file_key_parts[3], "%m%d%Y").date()
            end_scraper_date = datetime.strptime(file_key_parts[5], "%m%d%Y").date()
        except Exception as e:
            raise ValueError(f"Error parsing file key: {file_key}. Error: {str(e)}")
        
        return file_type, collection_id, start_scraper_date, end_scraper_date
    