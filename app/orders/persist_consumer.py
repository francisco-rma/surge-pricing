import logging
import os

from app.redis_client import redis_client
from app.redis_persist import StreamSave

ORDER_STREAM = os.getenv("ORDER_REDIS_STREAM", "order_stream")
CONSUMER_GROUP_NAME = "order_persist_consumer_group"


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()


def main():
    with redis_client() as client:
        saver = StreamSave(
            client,
            stream_name=ORDER_STREAM,
            consumer_group_name=CONSUMER_GROUP_NAME,
        )
        saver.run()


if __name__ == "__main__":
    main()
