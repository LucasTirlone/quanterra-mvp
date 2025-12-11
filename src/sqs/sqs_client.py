# app/infra/sqs_client.py
import boto3
import logging

logger = logging.getLogger(__name__)


class SQSClient:
    def __init__(self, region_name: str | None = None):
        self._region = region_name or "us-east-1"
        self._client = boto3.client("sqs", region_name=self._region)

    @property
    def client(self):
        return self._client

    def receive_messages(
        self,
        queue_url: str,
        max_number: int = 10,
        wait_time_seconds: int = 20,
        visibility_timeout: int | None = None,
    ) -> list[dict]:
        params = {
            "QueueUrl": queue_url,
            "MaxNumberOfMessages": max_number,
            "WaitTimeSeconds": wait_time_seconds,
            "MessageAttributeNames": ["All"],
        }
        if visibility_timeout is not None:
            params["VisibilityTimeout"] = visibility_timeout

        resp = self._client.receive_message(**params)
        return resp.get("Messages", [])

    def delete_message(self, queue_url: str, receipt_handle: str):
        self._client.delete_message(
            QueueUrl=queue_url, ReceiptHandle=receipt_handle
        )

    def send_message(self, queue_url: str, body: str, message_attributes: dict | None = None):
        params = {
            "QueueUrl": queue_url,
            "MessageBody": body,
        }
        if message_attributes:
            params["MessageAttributes"] = message_attributes
        return self._client.send_message(**params)
