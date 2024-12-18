from datetime import datetime

import h3

from app.data_aggregator_service import DataAggregator
from app.driver_position.aggregator_consumer import DRIVER_COUNT_KEY
from app.driver_position.schemas import DriverPositionsCount

TIME_WINDOW_MINUTES = 5


class DriverPositionAggregator(DataAggregator):
    def __init__(self, redis_client, time_window_minutes=TIME_WINDOW_MINUTES):
        super().__init__(
            redis_client,
            key_prefix=DRIVER_COUNT_KEY,
            time_window_minutes=time_window_minutes,
        )

    def get_driver_count_for_all_cells(self, cell_resolution: int):
        """Fetch and return driver count for all cells."""
        return self.get_aggregated_data(cell_resolution)

    def get_driver_count_in_last_minute(self, cell_id: str):
        return self.get_count_in_last_minute(cell_id=cell_id)
