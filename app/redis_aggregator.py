import json
import logging
import time

import h3
import numpy as np

from app.redis_processor import StreamProcessor

STREAM_READ_TIMEOUT = 2000
SLEEP_INTERVAL = 0.1
BATCH_SIZE = 10
CLAIM_INTERVAL = 60

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


class StreamAggregator(StreamProcessor):
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
        super().__init__(
            redis_client, stream_name, consumer_group_name, batch_size, claim_interval
        )
        self.resolutions = resolutions
        self.key_prefix = key_prefix

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

    def consume_messages(self):
        logger.info("Starting Aggregator...")

        response = self.client.xreadgroup(
            self.consumer_group_name,
            "agg_consumer_1",
            {self.stream_name: ">"},  # '>' means read only new messages
            count=self.batch_size,
            block=STREAM_READ_TIMEOUT,
        )
        if response:
            stream_name, messages = response[0]
            logger.info(f"Processing {len(messages)} messages from {stream_name}")
            for message_id, data in messages:
                try:
                    latitude = float(data["latitude"])
                    longitude = float(data["longitude"])
                    timestamp = data["timestamp"]
                    h3_cells = self.get_h3_cells(latitude, longitude)
                    self.update_count(h3_cells, timestamp)
                    logger.debug(f"Updated counts for {h3_cells} at {timestamp}")
                    self.client.xack(
                        self.stream_name, self.consumer_group_name, message_id
                    )
                except Exception as e:
                    logger.error(f"Error processing message {message_id}: {e}")
        else:
            logger.info("No new messages, sleeping...")
            time.sleep(SLEEP_INTERVAL)
