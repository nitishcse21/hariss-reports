from pydantic import BaseModel
from typing import Optional,List
from datetime import date

class AttendanceRequest(BaseModel):
    from_date: str
    to_date: str
    search_type: str = "Projects"     
    warehouse_ids: Optional[List[int]] = None
    salesman_ids: Optional[List[int]] = None


