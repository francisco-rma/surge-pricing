import logging
import time
import uuid
from datetime import datetime

from cassandra.cluster import Cluster

from app.redis_processor import StreamProcessor
from cassandra_db.cassandra_connection import create_cassandra_connection

STREAM_READ_TIMEOUT = 2000
SLEEP_INTERVAL = 0.1
BATCH_SIZE = 10
CLAIM_INTERVAL = 60

# Setting up logging configuration
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
        batch_size=BATCH_SIZE,
        claim_interval=CLAIM_INTERVAL,
    ):
        super().__init__(
            redis_client, stream_name, consumer_group_name, batch_size, claim_interval
        )
        self.db_connection = create_cassandra_connection()
        self.session = self.db_connection

    def save_to_db(self, data):
        """This method saves the processed data to the Cassandra database."""
        try:

            driver_id = uuid.UUID(data.get("driver_id"))
            latitude = float(data.get("latitude"))
            longitude = float(data.get("longitude"))
            timestamp = datetime.strptime(data.get("timestamp"), "%Y-%m-%dT%H:%M:%S.%f")

            insert_query = """
                INSERT INTO driver_position (driver_id, latitude, longitude, timestamp)
                VALUES (?, ?, ?, ?)
            """
            prepared = self.session.prepare(insert_query)
            self.session.execute(prepared, (driver_id, latitude, longitude, timestamp))
            logger.info(f"Saved driver position for driver {driver_id} at {timestamp}")

        except Exception as e:
            logger.error(f"Error saving to Cassandra: {e}")

    def consume_messages(self):
        """This method consumes messages from Redis Stream and saves them to Cassandra."""
        logger.info("Starting Saver...")

        while True:
            try:
                response = self.client.xreadgroup(
                    self.consumer_group_name,
                    "persist_consumer_1",
                    {self.stream_name: ">"},  # '>' means read only new messages
                    count=self.batch_size,
                    block=STREAM_READ_TIMEOUT,
                )
                if response:
                    stream_name, messages = response[0]
                    logger.info(
                        f"Processing {len(messages)} messages from {stream_name}"
                    )
                    for message_id, data in messages:
                        try:
                            self.save_to_db(data)
                            self.client.xack(
                                self.stream_name, self.consumer_group_name, message_id
                            )
                        except Exception as e:
                            logger.error(f"Error processing message {message_id}: {e}")
                else:
                    logger.info("No new messages, sleeping...")
                    time.sleep(SLEEP_INTERVAL)
            except Exception as e:
                logger.error(f"Error reading from stream: {e}")
                time.sleep(SLEEP_INTERVAL)
