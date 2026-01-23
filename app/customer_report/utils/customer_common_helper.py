from app.customer_report.schemas.customer_sales_schema import FilterSelection
from fastapi import HTTPException
from datetime import datetime
from typing import Optional, List, Tuple, Dict



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




def build_query_parts(
    filters: FilterSelection,
) -> Tuple[List[str], List[str], Dict]:


    joins: List[str] = []
    where_fragments: List[str] = []
    params: Dict = {}

    # --------------------------------------------------
    # Date range (ALWAYS REQUIRED)
    # --------------------------------------------------
    where_fragments.append(
        "ih.invoice_date BETWEEN :from_date AND :to_date"
    )
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.display_quantity and filters.display_quantity.lower() == "without_free_good":
        where_fragments.append("id.item_total > 0")
    # --------------------------------------------------
    # Company filter (direct on invoice_headers)
    # --------------------------------------------------
    if filters.company_ids:
        joins.append(
            "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
        )
        where_fragments.append(
            "ih.company_id = ANY(:company_ids)"
        )
        params["company_ids"] = filters.company_ids

    # --------------------------------------------------
    # Region / Area / Warehouse hierarchy
    # Requires warehouse join
    # --------------------------------------------------
    need_warehouse_join = False

    if filters.region_ids:
        joins.append(
            "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
        )
        where_fragments.append(
            "w.region_id = ANY(:region_ids)"
        )
        params["region_ids"] = filters.region_ids
        need_warehouse_join = True

    if filters.area_ids:
        if not need_warehouse_join:
            joins.append(
                "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
            )
            need_warehouse_join = True

        where_fragments.append(
            "w.area_id = ANY(:area_ids)"
        )
        params["area_ids"] = filters.area_ids

    if filters.warehouse_ids:
        where_fragments.append(
            "ih.warehouse_id = ANY(:warehouse_ids)"
        )
        params["warehouse_ids"] = filters.warehouse_ids

    # --------------------------------------------------
    # Route filter (direct on invoice_headers)
    # --------------------------------------------------
    if filters.route_ids:
        where_fragments.append(
            "ih.route_id = ANY(:route_ids)"
        )
        params["route_ids"] = filters.route_ids
        
    # --------------------------------------------------
    # De-duplicate JOINs
    # --------------------------------------------------
    joins = list(dict.fromkeys(joins))

    return joins, where_fragments, params
