import logging
import time

import h3
import redis

from app.redis_client import redis_client

STREAM_READ_TIMEOUT = 2000
SLEEP_INTERVAL = 0.1
BATCH_SIZE = 10
CLAIM_INTERVAL = 60

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


class StreamProcessor:
    def __init__(
        self,
        redis_client,
        stream_name,
        consumer_group_name,
        batch_size=BATCH_SIZE,
        claim_interval=CLAIM_INTERVAL,
    ):
        self.client = redis_client
        self.stream_name = stream_name
        self.consumer_group_name = consumer_group_name
        self.batch_size = batch_size
        self.claim_interval = claim_interval
        self.last_claim_time = 0

    def create_consumer_group(self):
        """Create the consumer group if it doesn't exist."""
        try:
            self.client.xgroup_create(
                self.stream_name, self.consumer_group_name, id="0"
            )
            logger.info("Consumer group created successfully.")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info("Consumer group already exists.")
            else:
                logger.error(f"Unexpected error creating consumer group: {e}")

    def claim_unacknowledged_messages(
        self, new_consumer="consumer_1", min_idle_time=60000
    ):
        try:
            pending_info = self.client.xpending_range(
                self.stream_name,
                self.consumer_group_name,
                min="-",
                max="+",
                count=10,
                consumer=None,
            )

            if pending_info:
                logger.info(f"Claiming {len(pending_info)} pending messages...")
                for msg in pending_info:
                    message_id = msg["message_id"]
                    reclaimed_messages = self.client.xclaim(
                        self.stream_name,
                        self.consumer_group_name,
                        new_consumer,
                        min_idle_time,
                        message_id,
                    )
                    if reclaimed_messages:
                        logger.info(f"Message {message_id} successfully claimed.")
                    else:
                        logger.warning(f"Failed to claim message {message_id}.")
            else:
                logger.info("No pending messages to claim.")
        except Exception as e:
            logger.error(f"Error processing pending messages: {e}")

    def consume_messages(self):
        """Base method to be overridden in subclasses to process messages."""
        raise NotImplementedError(
            "consume_messages method must be implemented by subclasses."
        )

    def run(self):
        self.create_consumer_group()

        try:
            while True:
                current_time = time.time()
                if current_time - self.last_claim_time >= self.claim_interval:
                    self.claim_unacknowledged_messages()
                    self.last_claim_time = current_time

                self.consume_messages()
        except KeyboardInterrupt:
            logger.info("Processor stopped by user.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection error: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
        finally:
            logger.info("Shutting down.")
