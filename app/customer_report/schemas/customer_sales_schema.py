from datetime import date
from typing import Optional, List
from pydantic import BaseModel



class FilterSelection(BaseModel):
    from_date: str
    to_date: str
    search_type: str = "quantity"                
    company_ids: Optional[List[int]] = None
    region_ids: Optional[List[int]] = None
    area_ids: Optional[List[int]] = None
    warehouse_ids: Optional[List[int]] = None
    route_ids: Optional[List[int]] = None
    display_quantity: Optional[str] = None 



class DownloadRequest(BaseModel):
    from_date: date
    to_date: date
    search_type: str                 # quantity | amount
    view_type: str                   # default | detail
    file_type: str                   # csv | xlsx

    display_quantity: str            # with_free_good | without_free_good

    company_ids: Optional[List[int]] = None
    region_ids: Optional[List[int]] = None
    area_ids: Optional[List[int]] = None
    warehouse_ids: Optional[List[int]] = None
    route_ids: Optional[List[int]] = None
