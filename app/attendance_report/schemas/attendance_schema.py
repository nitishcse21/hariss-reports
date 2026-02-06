from pydantic import BaseModel
from typing import Optional,List

class AttendanceRequest(BaseModel):
    from_date: str
    to_date: str
    search_type: str = "projects"     
    warehouse_ids: Optional[List[int]] = None
    salesman_ids: Optional[List[int]] = None