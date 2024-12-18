import logging
import os
import random
import signal
import time
import uuid
from contextlib import contextmanager
from datetime import datetime

import redis

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))


@contextmanager
def redis_client():
    """Context manager for Redis client."""
    client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    try:
        logger.info(f"Connecting to Redis at {REDIS_HOST}:{REDIS_PORT}")
        yield client
    except redis.ConnectionError as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise
    finally:
        logger.info("Closing Redis connection.")
        client.close()
