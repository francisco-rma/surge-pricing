import logging

from app.redis_aggregator import StreamAggregator
from app.redis_client import redis_client

DRIVER_POSITION_STREAM = "driver_position_stream"
DRIVER_COUNT_KEY = "driver_count_by_region"

RESOLUTIONS = [7, 8, 9]
CONSUMER_GROUP_NAME = "driver_position_consumer_group"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


def main():
    with redis_client() as client:
        aggregator = StreamAggregator(
            client,
            stream_name=DRIVER_POSITION_STREAM,
            consumer_group_name=CONSUMER_GROUP_NAME,
            resolutions=RESOLUTIONS,
            key_prefix=DRIVER_COUNT_KEY,
        )
        aggregator.run()


if __name__ == "__main__":
    main()
