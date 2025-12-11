import logging
import os
import boto3

from sqs.base_sqs_consumer import BaseSQSConsumer
from service.s3 import LocalFolderToS3CsvService
from partner.chain_scrapes_api import generate_chain_scrape_in_intervals
from partner.collection_api import generate_reports_in_intervals
from service.file_event_service import create_file_event_log_for_uploaded
from service.db_service import get_db_session

from datetime import datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

sqs = boto3.client("sqs", region_name="us-east-1")

collection_id = 303288
session = get_db_session()


class PartnerIntegrationConsumer(BaseSQSConsumer):
    queue_name = os.getenv("SQS_QUEUE_PARTNER_INTEGRATION")
    
    resp = sqs.get_queue_url(QueueName=queue_name)
    queue_url = resp["QueueUrl"]

    def handle(self, payload, message_attributes, raw_message):
        now = datetime.now().date()
        start_date = now - timedelta(weeks=1)
        end_date = now
        
        folder = "./download/raw/"
        collection_file_name = f"collection_{collection_id}_scrape_{start_date}_to_{end_date}.xlsx"
        chain_scrape_file_name = f"chainscrapes_{collection_id}_scrape_{start_date}_to_{end_date}.csv"
        
        generate_reports_in_intervals(collection_id, start_date, end_date, folder, collection_file_name)
        generate_chain_scrape_in_intervals(collection_id, start_date, end_date, folder, chain_scrape_file_name)
        
        LocalFolderToS3CsvService.upload_csvs_and_clean(
            s3_folder="raw/"
            local_folder=folder,
            recursive=False,
            dry_run=False,
        )
        
        create_file_event_log_for_uploaded(session, collection_file_name, collection_id, now)
        create_file_event_log_for_uploaded(session, chain_scrape_file_name, collection_id, now)
