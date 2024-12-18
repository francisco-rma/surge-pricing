import datetime
import logging
import os
import time

import h3
import redis

from app.redis_client import redis_client

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379

RESOLUTIONS = [7, 8, 9]
STREAM_READ_TIMEOUT = 2000
SLEEP_INTERVAL = 0.1
BATCH_SIZE = 10
CLAIM_INTERVAL = 60

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


class StreamAggregator:
    def __init__(
        self,
        redis_client,
        stream_name,
        consumer_group_name,
        resolutions,
        key_prefix,
        batch_size=BATCH_SIZE,
        claim_interval=CLAIM_INTERVAL,
    ):
        self.client = redis_client
        self.stream_name = stream_name
        self.consumer_group_name = consumer_group_name
        self.resolutions = resolutions
        self.key_prefix = key_prefix
        self.batch_size = batch_size
        self.claim_interval = claim_interval
        self.last_claim_time = 0

    def get_h3_cells(self, latitude, longitude):
        return {
            res: h3.latlng_to_cell(latitude, longitude, res) for res in self.resolutions
        }

    def update_count(self, h3_cells, timestamp):
        time_key = timestamp[:16]
        with self.client.pipeline() as pipe:
            for res, h3_cell in h3_cells.items():
                resolution_key = f"{self.key_prefix}:{time_key}:{res}"
                logger.info(f"Resolution KEY {resolution_key}")
                pipe.hincrby(resolution_key, h3_cell, 1)
            pipe.execute()

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

    def consume_messages_and_aggregate(self):
        logger.info("Starting Aggregator...")

        try:
            while True:
                response = self.client.xreadgroup(
                    self.consumer_group_name,
                    "consumer_1",
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
                            latitude = float(data["latitude"])
                            longitude = float(data["longitude"])
                            timestamp = data["timestamp"]
                            h3_cells = self.get_h3_cells(latitude, longitude)
                            self.update_count(h3_cells, timestamp)
                            logger.debug(
                                f"Updated counts for {h3_cells} at {timestamp}"
                            )
                            self.client.xack(
                                self.stream_name, self.consumer_group_name, message_id
                            )
                        except Exception as e:
                            logger.error(f"Error processing message {message_id}: {e}")
                else:
                    logger.info("No new messages, sleeping...")
                    time.sleep(SLEEP_INTERVAL)
        except KeyboardInterrupt:
            logger.info("Aggregator stopped by user.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection error: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
        finally:
            logger.info("Aggregator shutting down.")

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

    def run(self):
        self.create_consumer_group()

        try:
            while True:
                current_time = time.time()
                if current_time - self.last_claim_time >= self.claim_interval:
                    self.claim_unacknowledged_messages()
                    self.last_claim_time = current_time

                self.consume_messages_and_aggregate()
        except KeyboardInterrupt:
            logger.info("Aggregator stopped by user.")
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection error: {e}")
        except Exception as e:
            logger.exception(f"Unexpected error: {e}")
        finally:
            logger.info("Shutting down.")
