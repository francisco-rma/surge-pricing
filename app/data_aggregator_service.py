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


class DataAggregator:
    def __init__(
        self, redis_client, key_prefix, time_window_minutes=TIME_WINDOW_MINUTES
    ):
        self.client = redis_client
        self.key_prefix = key_prefix
        self.time_window_minutes = time_window_minutes

    def _generate_time_keys(self):
        """Generate time keys for the last `time_window_minutes`."""
        current_time = datetime.utcnow()
        return [
            (current_time - timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M")
            for i in range(self.time_window_minutes)
        ]

    def _aggregate_counts(self, time_keys, cell_resolution):
        """Aggregate the counts for the given time keys and cell resolution."""
        total_count = {}

        with self.client.pipeline() as pipe:
            for time_key in time_keys:
                resolution_key = f"{self.key_prefix}:{time_key}:{cell_resolution}"
                pipe.hgetall(resolution_key)

            results = pipe.execute()

        for data in results:
            if not data:
                continue
            for region, count in data.items():
                total_count[region] = total_count.get(region, 0) + int(count)

        return total_count

    def get_aggregated_data(self, cell_resolution: int):
        """Fetch and aggregate data for the specified H3 resolution."""
        time_keys = self._generate_time_keys()
        total_count = self._aggregate_counts(time_keys, cell_resolution)

        aggregated_data = [
            DriverPositionsCount(region=region, count=count)
            for region, count in total_count.items()
        ]

        return DriverPositionsCountResponse(driver_position_counts=aggregated_data)
