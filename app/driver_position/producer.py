import json
import logging
import os
import random
import signal
import time
import uuid
import numpy as np
from datetime import datetime

from dotenv import load_dotenv
import redis
import redis.client


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
# positions = np.zeros((TOTAL_DRIVERS, 3))
# positions[:, 0] = rng.uniform(low=BH_LAT_MIN, high=BH_LAT_MAX)
# positions[:, 1] = rng.uniform(low=BH_LON_MIN, high=BH_LON_MAX)
# positions[:, 1] = [time.time()] * len(positions)


def generate_driver_position(client: redis.Redis):
    """Generate a driver's position within Belo Horizonte's coordinates."""
    # driver_id = uuid.uuid4()
    driver_id = rng.choice(TOTAL_DRIVERS)
    driver_data = json.loads(client.get(str(driver_id)))
    logger.info(driver_data)

    deltatime = time.time() - driver_data["timestamp"]

    old_lat = float(driver_data["latitude"])
    lat_switch = None
    match old_lat:
        case old_lat if old_lat < BH_LAT_MIN:
            lat_switch = 1
        case old_lat if old_lat > BH_LAT_MAX:
            lat_switch = -1
        case _:
            lat_switch = rng.choice([-1, 1])

    old_lon = float(driver_data["longitude"])
    lon_switch = None
    match old_lon:
        case old_lon if old_lon < BH_LON_MIN:
            lon_switch = 1
        case old_lon if old_lon > BH_LON_MAX:
            lon_switch = -1
        case _:
            lon_switch = rng.choice([-1, 1])

    new_lat = old_lat + lat_switch * deltatime * (10**-5)
    new_lon = old_lon + lon_switch * deltatime * (10**-5)

    logger.info(f"Delta time: {deltatime}")
    logger.info(f"Old lat: {new_lat}")
    logger.info(f"New lat: {driver_data['latitude']}")
    logger.info(f"Old lon: {driver_data['longitude']}")
    logger.info(f"New lon: {new_lon}")
    driver_data["latitude"] = new_lat
    driver_data["longitude"] = new_lon
    driver_data["timestamp"] = time.time() # test

    client.set(
        driver_id,
        json.dumps(
            {
                "latitude": f"{driver_data['latitude']}",
                "longitude": f"{driver_data['longitude']}",
                "timestamp": driver_data["timestamp"],
            }
        ),
    )

    return {
        "driver_id": str(driver_id),
        "latitude": f"{driver_data['latitude']}",
        "longitude": f"{driver_data['longitude']}",
        "timestamp": driver_data["timestamp"],
    }


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    positions = np.zeros((TOTAL_DRIVERS, 3))
    positions[:, 0] = rng.uniform(low=BH_LAT_MIN, high=BH_LAT_MAX)
    positions[:, 1] = rng.uniform(low=BH_LON_MIN, high=BH_LON_MAX)
    positions[:, 2] = [time.time()] * len(positions)

    with redis_client() as client:
        logger.info("Starting DriverPosition producer...")
        pipe = client.pipeline()
        for idx, row in enumerate(positions):
            data = json.dumps(
                {
                    "driver_id": str(),
                    "latitude": f"{positions[idx][0]}",
                    "longitude": f"{positions[idx][1]}",
                    "timestamp": positions[idx][2],
                }
            )
            pipe.set(str(idx), data)
        pipe.execute()

        # Driver position producer
        driver_position_producer = RedisProducer(
            client=client,
            stream_name=DRIVER_POSITION_STREAM,
            generate_data_callback=generate_driver_position,
        )

        driver_position_producer.produce()
