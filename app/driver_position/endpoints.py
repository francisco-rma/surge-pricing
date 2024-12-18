from fastapi import APIRouter, Query

from app.driver_position.schemas import DriverPositionsCountResponse
from app.driver_position.service import \
    get_real_time_driver_count_for_all_cells

router = APIRouter()


@router.get("/driver_count", response_model=DriverPositionsCountResponse)
def driver_count(cell_resolution: int = Query(..., description="H3 cell resolution")):
    """API endpoint to get the real-time driver count."""
    driver_count_data = get_real_time_driver_count_for_all_cells(
        cell_resolution=cell_resolution
    )
    return driver_count_data
