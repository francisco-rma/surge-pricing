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
from app.redis_producer import RedisProducer, signal_handler

# Load environment variables from the .env file
load_dotenv()

# Coordinates for Belo Horizonte (central area)
BH_LAT_MIN = -20.0047113796
BH_LAT_MAX = -19.7890619963
BH_LON_MIN = -44.0986149944
BH_LON_MAX = -43.860692326

ORDER_STREAM = os.getenv("ORDER_REDIS_STREAM", "order_stream")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

shutdown_flag = False


def generate_order():
    order_id = uuid.uuid4()
    """Generate a order within Belo Horizonte's coordinates."""
    return {
        "order_id": str(order_id),
        "customer_id": str(uuid.uuid4()),
        "order_value": f"{random.uniform(10.0, 500.0):.2f}",
        "latitude": f"{random.uniform(BH_LAT_MIN, BH_LAT_MAX):.6f}",
        "longitude": f"{random.uniform(BH_LON_MIN, BH_LON_MAX):.6f}",
        "timestamp": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    with redis_client() as client:
        logger.info("Starting Orders producer...")

        # Driver position producer
        order = RedisProducer(
            client=client,
            stream_name=ORDER_STREAM,
            generate_data_callback=generate_order,
        )

        order.produce()
