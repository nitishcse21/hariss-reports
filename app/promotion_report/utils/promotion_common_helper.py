from fastapi import HTTPException
from typing import Optional,List,Tuple,Dict
from app.promotion_report.schemas.promotion_schema import PromotionRequest
from datetime import datetime



def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None
    
def validate_mandatory(filters:PromotionRequest):
    if not filters.from_date or not filters.to_date:
        raise HTTPException(status_code=400, detail="from_date and to_date are required")
    try:
        _ = datetime.fromisoformat(filters.from_date)
        _ = datetime.fromisoformat(filters.to_date)
    except Exception:
        raise HTTPException(status_code=400, detail="from_date/to_date must be in YYYY-MM-DD format")
    
def build_query_parts(
    filters: PromotionRequest,
) -> Tuple[List[str], List[str], Dict]:


    joins: List[str] = []
    where_fragments: List[str] = []
    params: Dict = {}

    where_fragments.append(
        "ih.invoice_date BETWEEN :from_date AND :to_date"
    )
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.company_ids:
        joins.append(
            "JOIN tbl_warehouse w ON w.id = ih.warehouse_id"
        )
        where_fragments.append(
            "ih.company_id = ANY(:company_ids)"
        )
        params["company_ids"] = filters.company_ids

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


    joins = list(dict.fromkeys(joins))

    return joins, where_fragments, params
