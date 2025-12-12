from importlib.resources import files
import logging
import os
import boto3

from sqs.base_sqs_consumer import BaseSQSConsumer
from service.s3 import S3CsvService
from service.file_event_service import create_file_event_log_for_uploaded, create_file_event_log_for_error

from datetime import datetime, timedelta

from service.location_service import update_location_status

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

sqs = boto3.client("sqs", region_name="us-east-1")

collection_id = 303288

s3_raw_bucket = os.getenv("S3_RAW_BUCKET_NAME")
s3_processed_bucket = os.getenv("S3_PROCESSED_BUCKET_NAME")


class ManualOpenCloseChainConsumer(BaseSQSConsumer):
    queue_name = os.getenv("SQS_QUEUE_MANUAL_OPEN_CLOSE_CHAIN")
    queue_url = f"https://sqs.us-east-1.amazonaws.com/461391639742/quan-prod-chain-files/{queue_name}"

    def handle(self, payload, raw_message, message_attributes ):
        current_file_key = ""
        now = datetime.now().date()

        try:
            folder = "./download/manual-open-close-chain/"
            
            files_key = S3CsvService.list_csv_files("open-close-chain", bucket_name=s3_raw_bucket)
            if not files_key:
                return
            
            for file_key in files_key:
                current_file_key = file_key
                file = S3CsvService.download_csv_file(current_file_key, "open-close-chain", folder, bucket_name=s3_raw_bucket)
                if not file:
                    continue        

                update_location_status(self.db_session, file)
                
                S3CsvService.clean_local_files(
                    local_folder=folder,
                    file_list=[file_key],
                    dry_run=False
                )
                
                S3CsvService.move_files(
                    bucket_name=s3_raw_bucket,
                    source_folder="open-close-chain/",
                    destination_folder="open-close-chain-processed/",
                    file_keys=[current_file_key],
                    dry_run=False
                )
                
                create_file_event_log_for_uploaded(self.db_session, current_file_key, None, now)
        
        except Exception as error:
            create_file_event_log_for_error(self.db_session, current_file_key, None, now, "MANUAL_OPEN_CLOSE_CHAIN", error)
            