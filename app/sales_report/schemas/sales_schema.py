
from typing import List, Optional
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
    salesman_ids: Optional[List[int]] = None
    item_category_ids: Optional[List[int]] = None
    item_ids: Optional[List[int]] = None
    customer_channel_ids: Optional[List[int]] = None
    customer_category_ids: Optional[List[int]] = None
    customer_ids: Optional[List[int]] = None
    display_quantity: Optional[str] = None 



class SalesReportRequest(BaseModel):
    from_date: str
    to_date: str
    search_type: Optional[str] = "quantity"
    dataview: Optional[str] = None
    company_ids: Optional[List[int]] = None
    region_ids: Optional[List[int]] = None
    area_ids: Optional[List[int]] = None
    warehouse_ids: Optional[List[int]] = None
    route_ids: Optional[List[int]] = None
    salesman_ids: Optional[List[int]] = None
    item_category_ids: Optional[List[int]] = None
    item_ids: Optional[List[int]] = None
    customer_channel_ids: Optional[List[int]] = None
    customer_category_ids: Optional[List[int]] = None
    customer_ids: Optional[List[int]] = None
    display_quantity: Optional[str] = None  




class DashboardFilter(FilterSelection):
    trend_granularity: Optional[str] = "daily"
    top_company_granularity: Optional[str] = "daily"
    main_group_granularity: Optional[str] = "daily"
    top_item_granularity: Optional[str] = "daily"
