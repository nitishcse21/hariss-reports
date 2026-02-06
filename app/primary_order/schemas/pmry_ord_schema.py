from pydantic import BaseModel
from typing import List, Optional


class PrimaryOrderReportSchema(BaseModel):
    from_date: str
    to_date: str
    company_ids: Optional[List[int]] = None
    region_ids: Optional[List[int]] = None
    area_ids: Optional[List[int]] = None
    warehouse_ids: Optional[List[int]] = None
    