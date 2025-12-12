# app/infra/base_consumer.py
import json
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

from sqs.sqs_client import SQSClient

logger = logging.getLogger(__name__)


class BaseSQSConsumer(ABC):
    """
    Base class for SQS consumers.

    Each subclass must define:
      - queue_url (str)
      - handle(payload, raw_message, message_attributes )
    """

    queue_url: str
    max_number_of_messages: int = 10
    wait_time_seconds: int = 20
    visibility_timeout = None

    def __init__(self, sqs_client = None):
        self.sqs = sqs_client or SQSClient()
        self._stopped = False

    @abstractmethod
    def handle(
        self,
        payload: Dict[str, Any],
        raw_message: Dict[str, Any],
        message_attributes = None,
    ):
        """
        Business logic to be executed for each message.
        """
        ...

    def stop(self):
        """
        Simple method to signal stop (can be used with signal handler).
        """
        self._stopped = True

    def _process_single_message(self, message: Dict[str, Any]):
        receipt_handle = message["ReceiptHandle"]
        raw_body = message["Body"]
        attributes = message.get("MessageAttributes")

        try:
            payload = json.loads(raw_body)
        except json.JSONDecodeError:
            logger.error("Invalid message (not JSON): %s", raw_body)
            # here you usually want to discard to avoid poison message
            self.sqs.delete_message(self.queue_url, receipt_handle)
            return

        logger.info(
            "Message received on %s: payload=%s, attrs=%s",
            self.queue_url,
            payload,
            attributes,
        )

        try:
            self.handle(payload, attributes, message)
            self.sqs.delete_message(self.queue_url, receipt_handle)
        except Exception:
            logger.exception("Error processing message from queue %s", self.queue_url)

    def start(self):
        """
        Simple consumption loop with long polling.
        """
        logger.info("Starting SQS consumer for queue: %s", self.queue_url)
        while not self._stopped:
            messages = self.sqs.receive_messages(
                queue_url=self.queue_url,
                max_number=self.max_number_of_messages,
                wait_time_seconds=self.wait_time_seconds,
                visibility_timeout=self.visibility_timeout,
            )

            if not messages:
                # nada na fila
                continue

            for msg in messages:
                self._process_single_message(msg)

        logger.info("SQS consumer completed for queue: %s", self.queue_url)
