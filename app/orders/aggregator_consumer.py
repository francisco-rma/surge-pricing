import logging
import os

from app.redis_aggregator import StreamAggregator
from app.redis_client import redis_client

ORDER_STREAM = os.getenv("ORDER_REDIS_STREAM", "order_stream")
ORDER_COUNT_KEY = "order_count_by_region"

RESOLUTIONS = [7, 8, 9]
CONSUMER_GROUP_NAME = "order_consumer_group"
CONSUMER_NAME = "consumer_1"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


def main():
    with redis_client() as client:
        aggregator = StreamAggregator(
            client,
            stream_name=ORDER_STREAM,
            consumer_group_name=CONSUMER_GROUP_NAME,
            resolutions=RESOLUTIONS,
            key_prefix=ORDER_COUNT_KEY,
        )
        aggregator.run()


if __name__ == "__main__":
    main()
