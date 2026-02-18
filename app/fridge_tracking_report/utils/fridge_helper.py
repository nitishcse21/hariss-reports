from typing import Optional, List, Tuple, Dict
from fastapi import HTTPException
from datetime import datetime
from app.fridge_tracking_report.schemas.fridge_schema import FridgeTrackingRequest

def parse_csv_ids(s: Optional[str]) -> Optional[List[int]]:
    if not s:
        return None
    parts = [p.strip() for p in s.split(",") if p.strip() != ""]
    try:
        return [int(p) for p in parts]
    except ValueError:
        return None
    

def validate_mandatory(filters: FridgeTrackingRequest):
    if not filters.from_date or not filters.to_date:
        raise HTTPException(status_code=400, detail="from_date and to_date are required")
    try:
        _ = datetime.fromisoformat(filters.from_date)
        _ = datetime.fromisoformat(filters.to_date)
    except Exception:
        raise HTTPException(status_code=400, detail="from_date/to_date must be in YYYY-MM-DD format")



def build_query_parts(
    filters: FridgeTrackingRequest,
) -> Tuple[List[str], List[str], Dict]:
 
    where_fragments: List[str] = []
    params: Dict = {}


    where_fragments.append(
        "ft.created_at BETWEEN :from_date AND :to_date"
    )
    params["from_date"] = filters.from_date
    params["to_date"] = filters.to_date

    if filters.fridge_available in ("yes", "no"):
        where_fragments.append("ft.have_fridge = :have_fridge")
        params["have_fridge"] = filters.fridge_available.lower()

    if filters.company_ids:
        where_fragments.append(
            "w.company IN :company_ids"
        )
        params["company_ids"] = tuple(
            str(i) for i in filters.company_ids
        )

    need_warehouse_join = False

    if filters.region_ids:
        where_fragments.append(
            "w.region_id = ANY(:region_ids)"
        )
        params["region_ids"] = filters.region_ids
        need_warehouse_join = True

    if filters.area_ids:
        if not need_warehouse_join:
            need_warehouse_join = True

        where_fragments.append(
            "w.area_id = ANY(:area_ids)"
        )
        params["area_ids"] = filters.area_ids

    if filters.warehouse_ids:
        where_fragments.append(
            "ac.warehouse = ANY(:warehouse_ids)"
        )
        params["warehouse_ids"] = filters.warehouse_ids



    return where_fragments, params

