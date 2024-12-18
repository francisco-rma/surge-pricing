from typing import Dict, List, Optional

from pydantic import BaseModel


class DriverPositionsCount(BaseModel):
    region: str
    count: int


class DriverPositionsCountResponse(BaseModel):
    driver_position_counts: Optional[List[DriverPositionsCount]]
