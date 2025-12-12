from importlib.resources import files
import logging
import os
import boto3
import pandas as pd

from sqs.base_sqs_consumer import BaseSQSConsumer
from service.s3 import S3CsvService
from service.file_event_service import create_file_event_log_for_uploaded, create_file_event_log_for_error

from datetime import datetime, timedelta

from service.quality_report_service import generate_quality_report_and_save
from service.report_service import generate_report_for_chain_scraper, generate_report_for_collection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

sqs = boto3.client("sqs", region_name="us-east-1")

collection_id = 303288

s3_raw_bucket = os.getenv("S3_RAW_BUCKET_NAME")
s3_processed_bucket = os.getenv("S3_PROCESSED_BUCKET_NAME")


class ReportGenerationConsumer(BaseSQSConsumer):
    queue_name = os.getenv("SQS_QUEUE_FILE_INGESTION")
    queue_url = f"https://sqs.us-east-1.amazonaws.com/461391639742/{queue_name}"

    def handle(self, payload, raw_message, message_attributes ):
        current_file_key = ""
        collection_id = None
        now = datetime.now().date()

        try:
            folder = "./download/raw-for-process/"
            
            files_key = S3CsvService.list_csv_files("raw", bucket_name=s3_raw_bucket)
            if not files_key:
                return
            
            for file_key in files_key:
                current_file_key = file_key
                file = S3CsvService.download_csv_file(current_file_key, "raw", folder, bucket_name=s3_raw_bucket)
                if not file:
                    continue        

                file_type, collection_id, start_scraper_date, end_scraper_date = self.__get_file_key_info(file_key)
                
                enriched_file_key = file_key.replace(".csv", "_enriched.csv")
                quality_file_key = file_key.replace(".csv", "_quality_report.csv")
                
                if file_type == "chainscrapes":
                    collection_file = S3CsvService.download_csv_file(current_file_key.replace("chainscrapes", "collection"), "raw", folder, bucket_name=s3_raw_bucket)
                    if not collection_file:
                        raise ValueError(f"Collection file not found for chainscrapes file: {file_key}")
                    
                    df_chain_scraper_csv = pd.read_csv(file)
                    df_collection_csv = pd.read_csv(collection_file)
                    generate_report_for_chain_scraper(self.db_session, collection_id, f"{folder}{enriched_file_key}", df_chain_scraper_csv, df_collection_csv)
                elif file_type == "collection":
                    df_collection_csv = pd.read_csv(file)
                    generate_report_for_collection(self.db_session, collection_id, f"{folder}{enriched_file_key}", start_scraper_date, end_scraper_date, df_collection_csv)
                    generate_quality_report_and_save(df_collection_csv, collection_id, f"{folder}{quality_file_key}")
                    S3CsvService.upload_csv(folder, quality_file_key, s3_processed_bucket, "healthcheck")
                else:
                    raise ValueError(f"Unknown file type: {file_type} in file key: {file_key}")
                    
                S3CsvService.upload_csv(folder, enriched_file_key, s3_processed_bucket, "exports")
                S3CsvService.move_files(
                    bucket_name=s3_raw_bucket,
                    source_folder="raw/",
                    destination_folder="raw-processed/",
                    file_keys=[current_file_key],
                    dry_run=False
                )
                S3CsvService.clean_local_files(folder, [file_key, enriched_file_key, quality_file_key])
                
                create_file_event_log_for_uploaded(self.db_session, current_file_key, collection_id, now)
        
        except Exception as error:
            create_file_event_log_for_error(self.db_session, current_file_key, collection_id, now, "REPORT", error)
    
    
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
    