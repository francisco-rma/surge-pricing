from app.data_aggregator_service import DataAggregator

TIME_WINDOW_MINUTES = 5


class OrderAggregator(DataAggregator):
    def __init__(self, redis_client, time_window_minutes=TIME_WINDOW_MINUTES):
        super().__init__(
            redis_client,
            key_prefix="order_count_by_region",
            time_window_minutes=time_window_minutes,
        )

    def get_order_count_for_all_cells(self, cell_resolution: int):
        """Fetch and return order count for all regions."""
        return self.get_aggregated_data(cell_resolution)

    def get_order_count_in_last_minute(self, cell_id: str):
        return self.get_count_in_last_minute(cell_id=cell_id)
