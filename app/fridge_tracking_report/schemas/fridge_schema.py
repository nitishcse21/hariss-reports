from pydantic import BaseModel
from typing import Optional, List


class FridgeTrackingRequest(BaseModel):
    from_date:str
    to_date:str
    fridge_available:str= "default"
    company_ids:Optional[List[int]] = None
    region_ids:Optional[List[int]] = None
    area_ids:Optional[List[int]] = None
    warehouse_ids:Optional[List[int]] = None