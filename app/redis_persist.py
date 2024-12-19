import logging
import time

import h3

from app.redis_processor import StreamProcessor

STREAM_READ_TIMEOUT = 2000
SLEEP_INTERVAL = 0.1
BATCH_SIZE = 10
CLAIM_INTERVAL = 60

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


class StreamSave(StreamProcessor):
    def __init__(
        self,
        redis_client,
        stream_name,
        consumer_group_name,
        db_connection,
        batch_size=BATCH_SIZE,
        claim_interval=CLAIM_INTERVAL,
    ):
        super().__init__(
            redis_client, stream_name, consumer_group_name, batch_size, claim_interval
        )
        self.db_connection = db_connection  # Your DB connection here

    def save_to_db(self, data):
        """This method saves the processed data to the database."""
        try:
            pass
        except Exception as e:
            logger.error(f"Error saving to database: {e}")

    def consume_messages(self):
        logger.info("Starting Saver...")

        response = self.client.xreadgroup(
            self.consumer_group_name,
            "persist_consumer_1",
            {self.stream_name: ">"},  # '>' means read only new messages
            count=self.batch_size,
            block=STREAM_READ_TIMEOUT,
        )
        if response:
            stream_name, messages = response[0]
            logger.info(f"Processing {len(messages)} messages from {stream_name}")
            for message_id, data in messages:
                try:
                    # In this example, we're directly saving the data to DB
                    self.save_to_db(data)
                    self.client.xack(
                        self.stream_name, self.consumer_group_name, message_id
                    )
                except Exception as e:
                    logger.error(f"Error processing message {message_id}: {e}")
        else:
            logger.info("No new messages, sleeping...")
            time.sleep(SLEEP_INTERVAL)
