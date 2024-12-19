import logging
import os
import random
import signal
import time
import uuid
import numpy as np
from datetime import datetime

from dotenv import load_dotenv

from app.redis_client import redis_client
from app.redis_producer import RedisProducer, signal_handler

# Load environment variables from the .env file
load_dotenv()

# Coordinates for Belo Horizonte (central area)
BH_LAT_MIN = -20.0047113796
BH_LAT_MAX = -19.7890619963
BH_LON_MIN = -44.0986149944
BH_LON_MAX = -43.860692326

TOTAL_DRIVERS = 1000

DRIVER_POSITION_STREAM = os.getenv("REDIS_STREAM", "driver_position_stream")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

shutdown_flag = False

rng = np.random.default_rng()
positions = np.zeros((TOTAL_DRIVERS, 3))
positions[:, 0] = rng.uniform(low=BH_LAT_MIN, high=BH_LAT_MAX)
positions[:, 1] = rng.uniform(low=BH_LON_MIN, high=BH_LON_MAX)
positions[:, 1] = [time.time()] * len(positions)


def generate_driver_position():
    """Generate a driver's position within Belo Horizonte's coordinates."""
    # driver_id = uuid.uuid4()
    driver_id = rng.choice(len(positions))
    deltatime = time.time() - positions[driver_id][2]

    new_lat = positions[driver_id][0] + rng.choice([-1, 1]) * 5 * deltatime
    new_lon = positions[driver_id][1] + rng.choice([-1, 1]) * 5 * deltatime

    positions[driver_id][0] = new_lat
    positions[driver_id][1] = new_lon
    positions[driver_id][2] = time.time()

    return {
        "driver_id": str(driver_id),
        "latitude": f"{positions[driver_id][0]:.6f}",
        "longitude": f"{positions[driver_id][1]:.6f}",
        "timestamp": time.time(),
    }


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    with redis_client() as client:
        logger.info("Starting DriverPosition producer...")

        # Driver position producer
        driver_position_producer = RedisProducer(
            client=client,
            stream_name=DRIVER_POSITION_STREAM,
            generate_data_callback=generate_driver_position,
        )

        driver_position_producer.produce()
