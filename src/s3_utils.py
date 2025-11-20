import os
import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

RAW_BUCKET = os.getenv('RAW_BUCKET')
EXPORTS_BUCKET = os.getenv('EXPORTS_BUCKET')

def upload_file_to_s3(file_name, bucket, object_name=None):
    s3_client = boto3.client('s3')
    if object_name is None:
        object_name = file_name
    try:
        s3_client.upload_file(file_name, bucket, object_name)
        logger.info(f"Arquivo {file_name} enviado para {bucket}/{object_name}")
    except ClientError as e:
        logger.error(e)
        raise


def download_file_from_s3(bucket, object_name, file_name=None):
    s3_client = boto3.client('s3')
    if file_name is None:
        file_name = object_name
    try:
        s3_client.download_file(bucket, object_name, file_name)
        logger.info(f"Arquivo {object_name} baixado de {bucket} para {file_name}")
    except ClientError as e:
        logger.error(e)
        raise
