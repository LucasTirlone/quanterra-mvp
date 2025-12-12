import logging
import os
import boto3

from sqs.base_sqs_consumer import BaseSQSConsumer
from service.s3 import S3CsvService
from partner.chain_scrapes_api import generate_chain_scrape_in_intervals
from partner.collection_api import generate_reports_in_intervals
from service.file_event_service import create_file_event_log_for_uploaded, create_file_event_log_for_error

from datetime import datetime, timedelta

from service.location_service import close_location_by_limit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

sqs = boto3.client("sqs", region_name="us-east-1")

collection_id = 303288

s3_raw_bucket = os.getenv("S3_RAW_BUCKET_NAME")


class PartnerIntegrationConsumer(BaseSQSConsumer):
    queue_name = os.getenv("SQS_QUEUE_PARTNER_INTEGRATION")
    queue_url = f"https://sqs.us-east-1.amazonaws.com/461391639742/{queue_name}"

    def handle(self, payload, raw_message, message_attributes ):
        now = datetime.now().date()
        collection_file_name = ""
        chain_scrape_file_name = ""

        try:
            start_date = now - timedelta(weeks=1)
            end_date = now

            folder = "./download/raw/"
            collection_file_name = f"collection_{collection_id}_scrape_{start_date.strftime('%m%d%Y')}_to_{end_date.strftime('%m%d%Y')}.xlsx"
            chain_scrape_file_name = f"chainscrapes_{collection_id}_scrape_{start_date.strftime('%m%d%Y')}_to_{end_date.strftime('%m%d%Y')}.csv"
        
            generate_reports_in_intervals(collection_id, start_date, end_date, folder, collection_file_name)
            generate_chain_scrape_in_intervals(collection_id, start_date, end_date, folder, chain_scrape_file_name)
        
            S3CsvService.upload_csvs_and_clean(
                s3_folder="raw/",
                bucket_name=s3_raw_bucket,
                local_folder=folder,
                recursive=False,
                dry_run=False
            )
        
            create_file_event_log_for_uploaded(self.db_session, collection_file_name, collection_id, now)
            create_file_event_log_for_uploaded(self.db_session, chain_scrape_file_name, collection_id, now)
        except Exception as error:
            create_file_event_log_for_error(self.db_session, collection_file_name, collection_id, now, "UPLOAD", error)
            create_file_event_log_for_error(self.db_session, chain_scrape_file_name, collection_id, now, "UPLOAD", error)
            
        close_location_by_limit(self.db_session, weeks=52)
            