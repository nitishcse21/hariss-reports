from pydantic import BaseModel
from typing import Optional

class CustDashboardRequest(BaseModel):
    from_date:str
    to_date:str 
    display_quantity: Optional[str] = None 
