from pydantic import BaseModel
from typing import List, Optional
from datetime import date

class OrderSummaryFilters(BaseModel):
    from_date: date
    to_date: date

    company_ids: Optional[List[int]] = None
    region_ids: Optional[List[int]] = None
    area_ids: Optional[List[int]] = None
    warehouse_ids: Optional[List[int]] = None
