import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime

import redis
from dotenv import load_dotenv

from app.redis_client import redis_client

# Load environment variables from the .env file
load_dotenv()

# Coordinates for Belo Horizonte (central area)
BH_LAT_MIN = -20.0047113796
BH_LAT_MAX = -19.7890619963
BH_LON_MIN = -44.0986149944
BH_LON_MAX = -43.860692326

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DRIVER_POSITION_STREAM = os.getenv("REDIS_STREAM", "driver_position_stream")
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


def generate_driver_position(driver_id):
    """Generate a driver's position within Belo Horizonte's coordinates."""
    return {
        "driver_id": str(driver_id),
        "latitude": f"{random.uniform(BH_LAT_MIN, BH_LAT_MAX):.6f}",
        "longitude": f"{random.uniform(BH_LON_MIN, BH_LON_MAX):.6f}",
        "timestamp": datetime.utcnow().isoformat(),
    }


def driver_position_producer(client):
    """Continuously produce driver positions, send them to a Redis stream, and publish via Pub/Sub."""
    try:
        while not shutdown_flag:
            driver_id = uuid.uuid4()
            driver_position = generate_driver_position(driver_id)

            try:
                with client.pipeline() as pipe:
                    # Add the position to the stream
                    pipe.xadd(DRIVER_POSITION_STREAM, driver_position)
                    pipe.execute()

                logger.info(f"Driver {driver_id} position sent: {driver_position}")

            except redis.RedisError as e:
                logger.error(f"Failed to send driver position to Redis: {e}")
                time.sleep(PRODUCE_INTERVAL * 2)  # Backoff before retrying

            time.sleep(PRODUCE_INTERVAL)

    except Exception as e:
        logger.exception(f"Unexpected error in producer: {e}")
    finally:
        logger.info("Driver position producer stopped.")


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    with redis_client() as client:
        logger.info("Starting driver position producer...")
        driver_position_producer(client)
