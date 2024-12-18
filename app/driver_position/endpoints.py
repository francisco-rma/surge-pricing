from fastapi import APIRouter, Query

from app.data_aggregator_service import REDIS_CLIENT
from app.driver_position.schemas import DriverPositionsCountResponse
from app.driver_position.service import DriverPositionAggregator

router = APIRouter()


@router.get("/driver_count", response_model=DriverPositionsCountResponse)
def driver_count(cell_resolution: int = Query(..., description="H3 cell resolution")):
    """API endpoint to get the real-time driver count."""

    # For Driver Positions
    driver_position_aggregator = DriverPositionAggregator(REDIS_CLIENT)
    return driver_position_aggregator.get_driver_count_for_all_cells(
        cell_resolution=cell_resolution
    )
