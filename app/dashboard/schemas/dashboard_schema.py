from pydantic import BaseModel
from typing import Optional,List

class DashboardRequest(BaseModel):
    from_date: str
    to_date: str
    company_ids: Optional[List[int]] = None
    region_ids: Optional[List[int]] = None
    area_ids: Optional[List[int]] = None
    warehouse_ids: Optional[List[int]] = None
    item_category_ids: Optional[List[int]] = None
    customer_channel_ids: Optional[List[int]] = None
    customer_category_ids: Optional[List[int]] = None
    display_quantity: Optional[str] = None 
