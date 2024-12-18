class SurgePricingCalculator:
    def __init__(self, base_price, driver_position_aggregator, order_aggregator):
        self.base_price = base_price
        self.driver_position_aggregator = driver_position_aggregator
        self.order_aggregator = order_aggregator

    def calculate_surge(self, h3_cell_id):
        """Calculate surge pricing based on driver and order count."""

        driver_count = self.driver_position_aggregator.get_driver_count_in_last_minute(
            h3_cell_id
        )

        order_count = self.order_aggregator.get_order_count_in_last_minute(h3_cell_id)

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
