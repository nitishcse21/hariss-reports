from pydantic import BaseModel
from typing import Optional

class LoadUnloadReportRequest(BaseModel):
    from_date: str
    to_date: str
    warehouse_id: Optional[int] = None
    salesman_id: Optional[int] = None
