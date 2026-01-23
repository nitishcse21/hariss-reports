from app.sales_report.schemas.sales_schema import FilterSelection
from fastapi import HTTPException
from datetime import datetime
from typing import Optional, List



def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None
    


def validate_mandatory(filters: FilterSelection):
    if not filters.from_date or not filters.to_date or not filters.search_type:
        raise HTTPException(status_code=400, detail="from_date, to_date, and search_type are required")
    try:
        _ = datetime.fromisoformat(filters.from_date)
        _ = datetime.fromisoformat(filters.to_date)
    except Exception:
        raise HTTPException(status_code=400, detail="from_date/to_date must be in YYYY-MM-DD format")




def build_query_parts(filters: FilterSelection):
    joins = []
    where_fragments = []
    params = {}

    where_fragments.append("ih.invoice_date BETWEEN :from_date AND :to_date")
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.display_quantity and filters.display_quantity.lower() == "without_free_good":
        where_fragments.append("id.item_total <> 0")

    if filters.company_ids:
        where_fragments.append("ih.company_id = ANY(:company_ids)")
        params["company_ids"] = filters.company_ids

    need_wh_join = False
    if filters.region_ids:
        joins.append("JOIN tbl_warehouse w ON w.id = ih.warehouse_id")
        where_fragments.append("w.region_id = ANY(:region_ids)")
        params["region_ids"] = filters.region_ids
        need_wh_join = True

    if filters.area_ids:
        if not need_wh_join:
            joins.append("JOIN tbl_warehouse w ON w.id = ih.warehouse_id")
            need_wh_join = True
        where_fragments.append("w.area_id = ANY(:area_ids)")
        params["area_ids"] = filters.area_ids

    if filters.warehouse_ids:
        where_fragments.append("ih.warehouse_id = ANY(:warehouse_ids)")
        params["warehouse_ids"] = filters.warehouse_ids

    if filters.route_ids:
        where_fragments.append("ih.route_id = ANY(:route_ids)")
        params["route_ids"] = filters.route_ids
        
    if filters.salesman_ids:
        where_fragments.append("ih.salesman_id = ANY(:salesman_ids)")
        params["salesman_ids"] = filters.salesman_ids

    if filters.item_category_ids:
        where_fragments.append("it.category_id = ANY(:item_category_ids)")
        params["item_category_ids"] = filters.item_category_ids

    if filters.item_ids:
        where_fragments.append("id.item_id = ANY(:item_ids)")
        params["item_ids"] = filters.item_ids

    # Customer filter WHERE only (joins handled in table grouping)
    if filters.customer_channel_ids:
        where_fragments.append("c.outlet_channel_id = ANY(:customer_channel_ids)")
        params["customer_channel_ids"] = filters.customer_channel_ids

    if filters.customer_category_ids:
        where_fragments.append("c.category_id = ANY(:customer_category_ids)")
        params["customer_category_ids"] = filters.customer_category_ids

    if filters.customer_ids:
        where_fragments.append("ih.customer_id = ANY(:customer_ids)")
        params["customer_ids"] = filters.customer_ids

    # Remove duplicates
    joins = list(dict.fromkeys(joins))
    return joins, where_fragments, params



