import logging
import os
import random
import signal
import uuid
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

ORDER_STREAM = os.getenv("ORDER_REDIS_STREAM", "order_stream")

BH_LAT_CENTER = -19.9191
BH_LON_CENTER = -43.9386

LAT_STDDEV = 0.05
LON_STDDEV = 0.05


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
        "latitude": f"{random.gauss(BH_LAT_CENTER, LAT_STDDEV):.6f}",
        "longitude": f"{random.gauss(BH_LON_CENTER, LON_STDDEV):.6f}",
        "timestamp": datetime.utcnow().isoformat(),
    }


if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    with redis_client() as client:
        logger.info("Starting Orders producer...")

        # Order Producer
        order = RedisProducer(
            client=client,
            stream_name=ORDER_STREAM,
            generate_data_callback=generate_order,
        )

        order.produce()
