import os
from datetime import datetime, timedelta

import redis

from app.driver_position.aggregator_consumer import DRIVER_COUNT_KEY
from app.driver_position.schemas import (DriverPositionsCount,
                                         DriverPositionsCountResponse)

# Redis connection configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = 6379
REDIS_CLIENT = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

TIME_WINDOW_MINUTES = 5


def get_real_time_driver_count_for_all_cells(
    cell_resolution: int = 8,
) -> DriverPositionsCountResponse:
    """Fetch and return driver count for the last 5 minutes at the specified H3 resolution."""
    current_time = datetime.utcnow()
    time_keys = [
        (current_time - timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M")
        for i in range(TIME_WINDOW_MINUTES)
    ]

    total_driver_count = {}

    with REDIS_CLIENT.pipeline() as pipe:
        for time_key in time_keys:
            resolution_key = f"{DRIVER_COUNT_KEY}:{time_key}:{cell_resolution}"
            pipe.hgetall(resolution_key)

        results = pipe.execute()

    for h3_data in results:
        if not h3_data:
            continue
        for h3_cell, count in h3_data.items():
            total_driver_count[h3_cell] = total_driver_count.get(h3_cell, 0) + int(
                count
            )

    driver_counts = [
        DriverPositionsCount(region=region, count=count)
        for region, count in total_driver_count.items()
    ]

    return DriverPositionsCountResponse(driver_position_counts=driver_counts)
