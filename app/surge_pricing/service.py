class SurgePricingCalculator:
    def __init__(self, base_price, driver_position_aggregator, order_aggregator):
        self.base_price = base_price
        self.driver_position_aggregator = driver_position_aggregator
        self.order_aggregator = order_aggregator

    def _calculate_surge_for_cell(self, order_count, driver_count):
        """Helper method to calculate surge price for a single cell."""
        if order_count == 0:
            return self.base_price

        ratio = order_count / driver_count if driver_count else 0

        if ratio < 1:
            surge_multiplier = 1
        elif ratio < 2:
            surge_multiplier = 1.2
        elif ratio < 3:
            surge_multiplier = 1.5
        else:
            surge_multiplier = 2

        return self.base_price * surge_multiplier

    def calculate_surge_for_all_cells(self, cell_resolution):
        """Calculate surge pricing for all cells given a resolution."""
        order_counts = self.order_aggregator.get_order_count_for_all_cells(
            cell_resolution
        )
        driver_counts = self.driver_position_aggregator.get_driver_count_for_all_cells(
            cell_resolution
        )

        order_counts = {
            data.region: data.count for data in order_counts.driver_position_counts
        }

        driver_counts = {
            data.region: data.count for data in driver_counts.driver_position_counts
        }

        surge_prices = {}
        for h3_cell_id in order_counts:
            order_count = order_counts[h3_cell_id]
            driver_count = driver_counts.get(h3_cell_id, 0)
            surge_prices[h3_cell_id] = self._calculate_surge_for_cell(
                order_count, driver_count
            )

        return surge_prices

    def calculate_surge(self, h3_cell_id):
        """Calculate surge pricing based on driver and order count."""
        driver_count = self.driver_position_aggregator.get_driver_count_in_last_minute(
            h3_cell_id
        )
        order_count = self.order_aggregator.get_order_count_in_last_minute(h3_cell_id)

        return self._calculate_surge_for_cell(order_count, driver_count)
