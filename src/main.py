from dotenv import load_dotenv
load_dotenv()

from secret_manager import update_secrets
update_secrets()

from worker.worker_manual_chain_open_close import ManualOpenCloseChainConsumer
from worker.worker_report_generation import ReportGenerationConsumer

import logging
import signal
import threading
from typing import Type

from sqs.sqs_client import SQSClient
from sqs.base_sqs_consumer import BaseSQSConsumer
from worker.worker_partner_integration import PartnerIntegrationConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)


class MainApp:

    def __init__(self):
        self.sqs_client = SQSClient()
        self.consumers: list[BaseSQSConsumer] = []
        self.threads: list[threading.Thread] = []
        self._stopping = False

    def register_consumer(self, consumer_cls: Type[BaseSQSConsumer]):
        """
        Registers a consumer based on the class.
        All use the same SQSClient (can change this if you want).
        """
        consumer = consumer_cls(self.sqs_client)
        self.consumers.append(consumer)
        logger.info("Consumer registered: %s", consumer_cls.__name__)

    def _run_consumer(self, consumer: BaseSQSConsumer):
        consumer.start()

    def start_all(self):
        """
        Starts all consumers in separate threads.
        """
        logger.info("Starting all consumers...")
        for consumer in self.consumers:
            t = threading.Thread(
                target=self._run_consumer, args=(consumer,), daemon=True
            )
            self.threads.append(t)
            t.start()
            logger.info("Thread started for %s", type(consumer).__name__)

        # Waits until a stop signal (Ctrl+C) or similar is received
        for t in self.threads:
            t.join()

    def stop_all(self, *args):
        """
        Called on shutdown (CTRL+C or SIGTERM).
        """
        if self._stopping:
            return
        self._stopping = True

        logger.info("Stop signal received. Stopping consumers...")
        for consumer in self.consumers:
            consumer.stop()

        logger.info("Stop signal sent to all consumers.")


def main():
    app = MainApp()

    # Here you decide which queues/micro-services this process will start.
    # Just register more consumers as you create them.
    app.register_consumer(PartnerIntegrationConsumer)
    app.register_consumer(ReportGenerationConsumer)
    app.register_consumer(ManualOpenCloseChainConsumer)

    # Configure signals for graceful shutdown
    signal.signal(signal.SIGINT, app.stop_all)
    signal.signal(signal.SIGTERM, app.stop_all)

    app.start_all()


if __name__ == "__main__":
    main()
