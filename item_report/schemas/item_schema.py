from typing import Optional, List
from pydantic import BaseModel



class FilterSelection(BaseModel):
    from_date: str
    to_date: str
    search_type: str = "quantity"    
    dataview: Optional[str] = None            
    company_ids: Optional[List[int]] = None
    region_ids: Optional[List[int]] = None
    area_ids: Optional[List[int]] = None
    warehouse_ids: Optional[List[int]] = None
    route_ids: Optional[List[int]] = None
    item_category_ids: Optional[List[int]] = None
    brand_ids: Optional[List[int]] = None
    item_ids: Optional[List[int]] = None
    display_quantity: Optional[str] = None 


