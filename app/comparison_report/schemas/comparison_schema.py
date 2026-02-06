from pydantic import BaseModel
from typing import Optional,List
from datetime import date

class ComparisonRequest(BaseModel):
    report_by: str   # "day" | "month" | "year"
    selected_date: date
    search_type: str = "quantity"     
    warehouse_ids: Optional[List[int]] = None
    salesman_ids: Optional[List[int]] = None
    display_quantity: Optional[str] = None  # with_free_good | without_free_good


