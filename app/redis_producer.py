import logging
import os
import time

import redis
from dotenv import load_dotenv

load_dotenv()

PRODUCE_INTERVAL = float(os.getenv("PRODUCE_INTERVAL", 1.0))

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

shutdown_flag = False


def signal_handler(sig, frame):
    global shutdown_flag
    logger.info("Shutdown signal received. Stopping producer...")
    shutdown_flag = True


class RedisProducer:
    def __init__(self, client, stream_name, generate_data_callback):
        """
        A general Redis producer that sends data to a Redis stream.

        Args:
            client: Redis client instance.
            stream_name: The name of the Redis stream.
            generate_data_callback: A function that generates data for the stream.
        """
        self.client = client
        self.stream_name = stream_name
        self.generate_data_callback = generate_data_callback

    def produce(self):
        """Continuously produce data and send it to the Redis stream."""
        global shutdown_flag
        try:
            while not shutdown_flag:
                data = self.generate_data_callback(self.client)
                try:
                    with self.client.pipeline() as pipe:
                        # Add data to the stream
                        pipe.xadd(self.stream_name, data)
                        pipe.execute()

                    logger.info(f"Data sent to {self.stream_name}: {data}")

                except redis.RedisError as e:
                    logger.error(f"Failed to send data to Redis: {e}")
                    time.sleep(PRODUCE_INTERVAL * 2)  # Backoff before retrying

                time.sleep(PRODUCE_INTERVAL)

        except Exception as e:
            logger.exception(f"Unexpected error in producer: {e}")
        finally:
            logger.info("Producer stopped.")
