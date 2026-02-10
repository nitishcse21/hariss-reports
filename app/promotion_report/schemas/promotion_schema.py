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