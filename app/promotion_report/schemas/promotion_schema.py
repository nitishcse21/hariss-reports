from pydantic import BaseModel
from typing import Optional,List

class PromotionRequest(BaseModel):
    from_date:str
    to_date:str
    status:str = "pending"
    company_ids:Optional[List]=None
    region_ids:Optional[List]=None
    area_ids:Optional[List]=None
    warehouse_ids:Optional[List]=None



class PromotionActionRequest(BaseModel):
    invoice_ids: List[int]
    action: str          # approve | reject
    comment: Optional[str] = None


class PromotionDateRangeRequest(BaseModel):
    from_date: str
    to_date: str